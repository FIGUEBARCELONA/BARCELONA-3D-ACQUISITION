#!/usr/bin/env python3
"""Bounded one-shot access check for the exact public ICGC COG; no proxy API."""
import hashlib
import http.client
import json
import os
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

HOST = 'datacloud.icgc.cat'
PATH = '/datacloud/model-elevacions-terreny/tif_unzip/model-elevacions-terreny-lidar-amb-25cm-2022-2022.tif'
TOTAL = 24310268511
LIMIT = 65536
REPORT = {'status': 'NOT_EXECUTED', 'native_cells_recovered': 0}


def probe():
    global REPORT
    out = {'status': 'FAILED', 'url': 'https://' + HOST + PATH,
           'expected_total_bytes': TOTAL, 'requested_bytes': LIMIT,
           'provider_region': os.getenv('RENDER_REGION'),
           'native_cells_recovered': 0, 'received_bytes': 0}
    started = time.monotonic()
    conn = http.client.HTTPSConnection(HOST, timeout=20, context=ssl.create_default_context())
    try:
        conn.request('GET', PATH, headers={'Range': f'bytes=0-{LIMIT-1}',
                                            'Accept-Encoding': 'identity',
                                            'Connection': 'close'})
        response = conn.getresponse()
        out['http_status'] = response.status
        out['content_range'] = response.getheader('Content-Range')
        out['content_length'] = response.getheader('Content-Length')
        out['etag'] = response.getheader('ETag')
        if response.status != 206 or out['content_range'] != f'bytes 0-{LIMIT-1}/{TOTAL}':
            raise ValueError('Exact range not supplied; no response body read')
        if out['content_length'] not in (None, str(LIMIT)):
            raise ValueError('Incorrect declared range length')
        if response.getheader('Content-Encoding') not in (None, 'identity'):
            raise ValueError('Unexpected content encoding')
        raw = response.read(LIMIT+1)
        out['received_bytes'] = len(raw)
        if len(raw) != LIMIT or raw[:4] not in (b'II*\x00', b'MM\x00*', b'II+\x00', b'MM\x00+'):
            raise ValueError('Incorrect TIFF prefix')
        out['header_sha256'] = hashlib.sha256(raw).hexdigest()
        out['status'] = 'RANGE_VERIFIED_NOT_NATIVE_CELLS'
    except Exception as e:
        out['error'] = str(e)
    finally:
        conn.close()
        out['elapsed_seconds'] = round(time.monotonic()-started, 3)
        REPORT = out
        print(json.dumps(out, sort_keys=True), flush=True)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/healthz':
            payload = {'status': 'SERVICE_UP', 'acquisition_status': REPORT['status']}
        elif self.path in ('/', '/report'):
            payload = REPORT
        else:
            self.send_error(404)
            return
        data = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(data)


def main():
    # Run one attempt per process, only in Render's runtime; no periodic task.
    # Local health tests never contact the external host.
    if os.getenv('RENDER') == 'true':
        threading.Thread(target=probe, daemon=True).start()
    HTTPServer(('0.0.0.0', int(os.environ.get('PORT', '10000'))), Handler).serve_forever()


if __name__ == '__main__':
    main()
