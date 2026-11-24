#!/usr/bin/env python3
"""One short, public scientific reproducibility check via documented BinderHub API.
No authentication secrets from GitHub are transmitted to Binder. No repeated launches,
no proxy service, no full raster download, no circumvention of launch restrictions.
"""
import json, time, uuid, datetime, pathlib, urllib.parse
import requests
import websocket

REF='b2514455bb2da6c2f9829aba962467fe85606ab6'
BUILD='https://mybinder.org/build/gh/FIGUEBARCELONA/BARCELONA-3D-ACQUISITION/'+REF
ROOT=pathlib.Path('custody/dm16')
ROOT.mkdir(parents=True,exist_ok=True)
report={'provider':'mybinder.org','source_commit':REF,'launch_attempts':1,
        'status':'NOT_EXECUTED','native_cells_recovered':0,'email_submitted':False,
        'production':'NO_GO','phases':[]}
# Same bounded independent probe, with no external credential or personal information.
PAYLOAD=r"""
import urllib.request,hashlib,json,time,ssl
url='https://datacloud.icgc.cat/datacloud/model-elevacions-terreny/tif_unzip/model-elevacions-terreny-lidar-amb-25cm-2022-2022.tif'
out={'status':'FAILED','received_bytes':0,'native_cells':0}
start=time.monotonic()
try:
    req=urllib.request.Request(url,headers={'Range':'bytes=0-65535','Accept-Encoding':'identity'})
    with urllib.request.urlopen(req,timeout=20,context=ssl.create_default_context()) as r:
        out.update(http=r.status,content_range=r.headers.get('Content-Range'),final_url=r.geturl())
        if r.status!=206 or out['content_range']!='bytes 0-65535/24310268511' or not r.geturl().startswith('https://'):
            raise ValueError('Exact range unavailable; no body read')
        raw=r.read(65537)
        out['received_bytes']=len(raw)
        if len(raw)!=65536 or raw[:4] not in (b'II*\x00',b'MM\x00*',b'II+\x00',b'MM\x00+'):
            raise ValueError('Unexpected TIFF prefix')
        out['status']='RANGE_VERIFIED_NOT_NATIVE_CELLS'
        out['header_sha256']=hashlib.sha256(raw).hexdigest()
except Exception as e:
    out['error']=str(e)
out['elapsed_s']=round(time.monotonic()-start,3)
print('BINDER_MDT_RESULT '+json.dumps(out))
"""
def main():
    deadline=time.monotonic()+150
    kernel=None; ws=None; session=None; base=None; token=None
    try:
        # Single launch, stop on service denial. Do not print event payloads containing tokens.
        with requests.get(BUILD,stream=True,timeout=(15,35)) as r:
            report['launch_http']=r.status_code
            report['launch_host']=urllib.parse.urlparse(r.url).hostname
            if r.status_code!=200:
                report['status']='LAUNCH_DENIED_OR_UNAVAILABLE'
                return
            ready=None
            for line in r.iter_lines(chunk_size=1):
                if time.monotonic()>deadline:
                    report['status']='LAUNCH_DEADLINE';return
                if not line.startswith(b'data:'):continue
                ev=json.loads(line[5:]);phase=ev.get('phase','unknown')
                if not report['phases'] or report['phases'][-1]!=phase:
                    report['phases'].append(phase)
                    print('Binder phase:',phase,flush=True)
                if phase=='failed':
                    report['status']='LAUNCH_FAILED';return
                if phase=='ready':ready=ev;break
        if ready is None:
            report['status']='NO_READY_EVENT';return
        base=ready['url'].rstrip('/')+'/'
        token=ready['token']
        parsed=urllib.parse.urlparse(base)
        if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password:
            report['status']='UNEXPECTED_SERVER_URL';return
        report['federation_server']=parsed.hostname
        session=requests.Session()
        session.headers['Authorization']='token '+token
        # Do not send token on redirect.
        response=session.post(base+'api/kernels',json={'name':'python3'},timeout=15,allow_redirects=False)
        report['kernel_http']=response.status_code
        if response.status_code!=201:
            report['status']='KERNEL_NOT_AVAILABLE';return
        kernel=response.json()['id']
        wsurl=base.replace('https://','wss://',1)+'api/kernels/'+kernel+'/channels?session_id='+uuid.uuid4().hex
        ws=websocket.create_connection(wsurl,header=['Authorization: token '+token],timeout=30,origin='https://'+parsed.netloc)
        msgid=uuid.uuid4().hex
        ws.send(json.dumps({'header':{'msg_id':msgid,'username':'public-research','session':uuid.uuid4().hex,'msg_type':'execute_request','version':'5.3','date':datetime.datetime.now(datetime.timezone.utc).isoformat()},'parent_header':{},'metadata':{},'content':{'code':PAYLOAD,'silent':False,'store_history':False,'user_expressions':{},'allow_stdin':False,'stop_on_error':True},'channel':'shell','buffers':[]}))
        report['status']='KERNEL_STARTED_NO_RESULT'
        until=time.monotonic()+35
        while time.monotonic()<until:
            m=json.loads(ws.recv())
            if m.get('parent_header',{}).get('msg_id')!=msgid:continue
            if m.get('msg_type')=='stream':
                for line in m.get('content',{}).get('text','').splitlines():
                    if line.startswith('BINDER_MDT_RESULT '):
                        report['probe']=json.loads(line[len('BINDER_MDT_RESULT '):])
                        report['status']='PROBE_COMPLETED'
                        return
            if m.get('msg_type')=='error':
                report['status']='KERNEL_EXECUTION_ERROR';return
    except Exception as e:
        # Exception text may contain server session identifiers; record only type.
        report['status']='ERROR';report['exception_type']=type(e).__name__
    finally:
        if ws:
            try:ws.close()
            except Exception:pass
        if kernel and session and base:
            try:report['kernel_cleanup_http']=session.delete(base+'api/kernels/'+kernel,timeout=10,allow_redirects=False).status_code
            except Exception:report['kernel_cleanup_http']='NOT_CONFIRMED'
        if session:session.close()
        (ROOT/'binder-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
        print('DM16_RESULT '+json.dumps(report,ensure_ascii=False),flush=True)
if __name__=='__main__':main()
