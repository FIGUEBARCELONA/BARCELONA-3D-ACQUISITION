import urllib.request,re,json,hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
base='https://visors.icgc.cat/appdownloads/'
names=['lib/customFunctions300926.js','lib/downloads300926.js','lib/controlsChangeRadioSerie070426.js']
def probe(n):
    u=base+n
    try:
        with urllib.request.urlopen(u,timeout=20) as r:
            b=r.read(2*1024*1024+1);assert len(b)<=2*1024*1024
        s=b.decode('utf-8',errors='replace')
        p=re.compile(r'(https?://[^\s"\'<>]+|function\s+\w*(?:download|fme|descarr)\w*\s*\([^)]*\)|\b(?:171|dwlElevationsSerieFme|GeoTiff|GetCoverage|FME_SERVER|FME_URL)\b)',re.I)
        hits=[{'match':m.group(0),'context':s[max(0,m.start()-140):min(len(s),m.end()+350)]} for m in list(p.finditer(s))[:70]]
        return {'url':u,'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b),'hits':hits}
    except Exception as e:return {'url':u,'error':str(e)}
with ThreadPoolExecutor(max_workers=3) as e: out=list(e.map(probe,names))
print('MDT_ROUTE_BEGIN');print(json.dumps(out,ensure_ascii=False,indent=2));print('MDT_ROUTE_END')
