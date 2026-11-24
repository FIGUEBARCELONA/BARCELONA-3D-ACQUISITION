import os, json, urllib.request, ssl, socket, time, hashlib, sys, platform, re
from pathlib import Path
URL="https://datacloud.icgc.cat/datacloud/model-elevacions-terreny/tif_unzip/model-elevacions-terreny-lidar-amb-25cm-2022-2022.tif"
LIMIT=65536
def probe():
    report={"platform":platform.platform(),"runner_os":os.getenv("RUNNER_OS"),"runner_arch":os.getenv("RUNNER_ARCH"),"url":URL,"expected_total_bytes":24310268511,"native_cells":0}
    t=time.monotonic()
    try:
        report["dns"]=sorted({r[4][0] for r in socket.getaddrinfo("datacloud.icgc.cat",443,type=socket.SOCK_STREAM)})
        req=urllib.request.Request(URL,headers={"Range":"bytes=0-65535","Accept-Encoding":"identity","User-Agent":"Barcelona3D public data QA"})
        with urllib.request.urlopen(req,timeout=20,context=ssl.create_default_context()) as res:
            report.update(http=res.status,content_range=res.headers.get("Content-Range"),content_length=res.headers.get("Content-Length"),final_url=res.geturl())
            if res.status!=206 or res.headers.get("Content-Range")!="bytes 0-65535/24310268511": raise ValueError("Range response not accepted; no full body read")
            if not res.geturl().startswith("https://"): raise ValueError("not HTTPS")
            b=res.read(LIMIT+1)
            report["received_bytes"]=len(b)
            if len(b)!=LIMIT or b[:4] not in (b"II*\x00",b"MM\x00*",b"II+\x00",b"MM\x00+"):raise ValueError("bad TIFF prefix")
            report["header_sha256"]=hashlib.sha256(b).hexdigest()
            report["range_verified"]=True
    except Exception as e:
        report["error"]=str(e);report["range_verified"]=False
    report["elapsed_s"]=round(time.monotonic()-t,3)
    report["independent_vertical_accuracy_verified"]=False
    print("DM14_RESULT "+json.dumps(report,ensure_ascii=False),flush=True)
    Path("dm14-probe.json").write_text(json.dumps(report))
    if report["range_verified"]:
        # Existing validated reader is invoked only after a successful bounded preflight.
        import subprocess
        subprocess.run([sys.executable,"-m","pip","install","--disable-pip-version-check","--no-cache-dir","numpy","requests","rasterio"],check=True,timeout=180)
        import mdt_native_dm12 as m
        try:
            result=m.native()
            print("DM14_NATIVE "+json.dumps(result,ensure_ascii=False,allow_nan=False),flush=True)
        except Exception as e: print("DM14_NATIVE_ERROR "+str(e),flush=True)
if __name__=="__main__":probe()
