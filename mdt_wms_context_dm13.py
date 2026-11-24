#!/usr/bin/env python3
"""Check exact layer point observations using the documented, successful 64-pixel WMS context.
No email, no synthetic grid, no full download. Public HTTP transport checked separately.
"""
import json,hashlib,math,requests
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
WMS='https://geoserveis.icgc.cat/servei/catalunya/elevacions-territorial/wms'
LAYER='model-elevacions-terreny-amb-lidar-25cm-2022'
URL='http://datacloud.icgc.cat/datacloud/model-elevacions-terreny/tif_unzip/model-elevacions-terreny-lidar-amb-25cm-2022-2022.tif'
X,Y=429177.375,4582885.875
def point(p):
    x,y=p
    # At I=32,J=32, center = xmin+32.5*.25,ymax-32.5*.25.
    q={'service':'WMS','version':'1.3.0','request':'GetFeatureInfo','layers':LAYER,'query_layers':LAYER,'styles':'','crs':'EPSG:25831','bbox':f'{x-8.125},{y-7.875},{x+7.875},{y+8.125}','width':64,'height':64,'i':32,'j':32,'format':'image/png','info_format':'application/json'}
    out={'requested_x':x,'requested_y':y}
    try:
        with requests.get(WMS,params=q,timeout=(10,20),stream=True) as r:
            b=r.raw.read(65537,decode_content=True)
            if len(b)>65536:raise ValueError('size cap')
            out.update(http=r.status_code,url=r.url,sha256=hashlib.sha256(b).hexdigest())
            j=json.loads(b); f=j.get('features',[]);out['feature_count']=len(f)
            if r.status_code!=200 or j.get('name')!=LAYER or len(f)!=1:raise ValueError('unexpected feature response')
            xx,yy=f[0]['geometry']['coordinates'];v=float(f[0]['properties']['Elevació'])
            if abs(xx-x)>1e-6 or abs(yy-y)>1e-6 or not math.isfinite(v):raise ValueError('invalid position/value')
            out.update(value_m=v,returned_x=xx,returned_y=yy)
    except Exception as e:out['error']=str(e)
    return out
def http_probe():
    try:
        # HEAD only: detect plain HTTP vs HTTPS availability without transferring the 24GB raster.
        with requests.head(URL,allow_redirects=False,timeout=(12,12)) as r:
            return {'http':r.status_code,'url':r.url,'headers':{k:v for k,v in r.headers.items() if k.lower() in ('content-length','location','accept-ranges','content-type','etag','last-modified')}}
    except Exception as e:return {'error':str(e)}
def main():
    # Three nearby points including the previously observed position. Never call them native cells.
    pts=[(X-.25,Y),(X,Y),(X+.25,Y)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        f=pool.submit(http_probe);obs=list(pool.map(point,pts));transport=f.result()
    r={'observations':obs,'valid':sum('value_m' in o for o in obs),'http_head':transport,'native_cells_recovered':0,'independent_accuracy_verified':False,'email_submitted':False,'production':'NO_GO'}
    path=Path('custody/dm13/wms-context-report.json');path.write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    print(json.dumps(r,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
