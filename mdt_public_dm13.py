#!/usr/bin/env python3
"""DM-13 public, no-email transport alternatives, WMS raster capability audit."""
import json,hashlib,math,os,sys,subprocess,xml.etree.ElementTree as ET
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import requests,rasterio,numpy as np
from rasterio.io import MemoryFile
OUT=Path('custody/dm13');OUT.mkdir(parents=True,exist_ok=True)
WMS='https://geoserveis.icgc.cat/servei/catalunya/elevacions-territorial/wms'
LAYER='model-elevacions-terreny-amb-lidar-25cm-2022'
ALIAS='https://datacloud.icgc.cat/datacloud/model-elevacions-terreny/vigent/tif_unzip/model-elevacions-terreny-lidar-amb-25cm.tif'
X,Y=429177.375,4582885.875
def sha(b):return hashlib.sha256(b).hexdigest()
def bounded(url,params=None,cap=2*1024*1024,headers=None):
    with requests.get(url,params=params,headers=headers,timeout=(12,20),stream=True) as r:
        b=r.raw.read(cap+1,decode_content=True)
        if len(b)>cap:raise ValueError('response larger than permitted cap')
        return r.status_code,r.url,dict(r.headers),b
def alias():
    # Independently published current-product URL; no download if no valid byte range.
    with requests.get(ALIAS,headers={'Range':'bytes=0-16383','Accept-Encoding':'identity'},stream=True,timeout=(12,20)) as r:
        out={'url':r.url,'http':r.status_code,'content_range':r.headers.get('Content-Range'),'bytes':0}
        if r.status_code==206 and r.headers.get('Content-Range')=='bytes 0-16383/24310268511':
            b=r.raw.read(16385);out['bytes']=len(b);out['sha256']=sha(b);out['tiff_header']=b[:4].hex()
            if len(b)!=16384 or b[:4] not in (b'II*\0',b'MM\0*',b'II+\0',b'MM\0+'):raise ValueError('invalid range bytes')
            with rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR',GDAL_HTTP_TIMEOUT='20',GDAL_HTTP_CONNECTTIMEOUT='12',GDAL_HTTP_MAX_RETRY='0'):
                with rasterio.open('/vsicurl/'+ALIAS) as ds:
                    row,col=ds.index(X,Y);win=rasterio.windows.Window(col-8,row-8,16,16)
                    if ds.crs.to_epsg()!=25831 or ds.res!=(.25,.25) or ds.dtypes[0]!='float32':raise ValueError('source metadata differs')
                    a=ds.read(1,window=win,masked=True)
                    if a.count()!=256 or not np.isfinite(a.compressed()).all():raise ValueError('incomplete window')
                    profile=dict(driver='GTiff',width=16,height=16,count=1,dtype='float32',crs=ds.crs,transform=ds.window_transform(win),nodata=ds.nodata)
                    with MemoryFile() as mem:
                        with mem.open(**profile) as dst:dst.write(a.data,1)
                        data=mem.read()
                    with MemoryFile(data) as mem:
                        with mem.open() as check:
                            if not np.array_equal(check.read(1).view('uint32'),a.data.view('uint32')):raise ValueError('window mismatch')
                    (OUT/'native-window.tif').write_bytes(data)
                    out['native_window']={'cells':256,'sha256':sha(data),'bounds':list(rasterio.windows.bounds(win,ds.transform)),'transform':list(profile['transform']),'min':float(a.min()),'max':float(a.max()),'source_shape':[ds.height,ds.width],'native_values_unchanged':True}
        return out
def wms():
    status,url,headers,b=bounded(WMS,{'SERVICE':'WMS','VERSION':'1.3.0','REQUEST':'GetCapabilities'})
    if status!=200:raise ValueError('WMS capabilities HTTP '+str(status))
    root=ET.fromstring(b);formats=[]
    for e in root.iter():
        if e.tag.split('}')[-1]=='GetMap':formats=[c.text for c in e if c.tag.split('}')[-1]=='Format']
    out={'capabilities_sha256':sha(b),'getmap_formats':formats,'tested':[],'native_cells':0}
    # GetMap TIFF is only accepted as a numerical service raster if Float32 and
    # independent of RGB styling. It is NOT the native COG.
    bbox=[X-2.125,Y-2.125,X+1.875,Y+1.875]
    for fmt in [f for f in formats if 'tiff' in f.lower() or 'geotiff' in f.lower()][:3]:
        q={'SERVICE':'WMS','VERSION':'1.3.0','REQUEST':'GetMap','LAYERS':LAYER,'STYLES':'','CRS':'EPSG:25831','BBOX':','.join(map(str,bbox)),'WIDTH':'16','HEIGHT':'16','FORMAT':fmt}
        try:
            st,u,h,raw=bounded(WMS,q,cap=1024*1024);item={'format':fmt,'url':u,'http':st,'content_type':h.get('Content-Type'),'bytes':len(raw),'sha256':sha(raw)}
            if raw[:4] in (b'II*\0',b'MM\0*',b'II+\0',b'MM\0+'):
                with MemoryFile(raw) as mem:
                    with mem.open() as ds:
                        arr=ds.read();item.update(bands=ds.count,dtypes=list(ds.dtypes),crs=ds.crs.to_string() if ds.crs else None,transform=list(ds.transform),shape=list(arr.shape),min=float(arr.min()),max=float(arr.max()))
                        if ds.count==1 and ds.dtypes[0]=='float32':item['center_value']=float(arr[0,7,8]);(OUT/'wms-numeric-candidate.tif').write_bytes(raw)
                        else:item['classification']='rendered/non-float raster; not elevation values'
            else:item['body_excerpt']=raw[:500].decode('utf8',errors='replace')
            out['tested'].append(item)
        except Exception as e:out['tested'].append({'format':fmt,'error':str(e)})
    return out
def point_check():
    results=[]
    # Nine nearby queries of the exact official layer; small bounded sample, not raster reconstruction.
    for dy in (0.25,0,-0.25):
        for dx in (-0.25,0,0.25):
            x,y=X+dx,Y+dy
            q={'SERVICE':'WMS','VERSION':'1.3.0','REQUEST':'GetFeatureInfo','LAYERS':LAYER,'QUERY_LAYERS':LAYER,'STYLES':'','CRS':'EPSG:25831','BBOX':f'{x-.125},{y-.125},{x+.125},{y+.125}','WIDTH':1,'HEIGHT':1,'I':0,'J':0,'FORMAT':'image/png','INFO_FORMAT':'application/json'}
            try:
                st,u,h,b=bounded(WMS,q,cap=65536);j=json.loads(b);f=j['features'][0];coords=f['geometry']['coordinates'];v=float(f['properties']['Elevació'])
                if st!=200 or j['name']!=LAYER or len(j['features'])!=1 or max(abs(coords[0]-x),abs(coords[1]-y))>1e-6 or not math.isfinite(v):raise ValueError('invalid point response')
                results.append({'x':x,'y':y,'value_m':v,'sha256':sha(b),'url':u})
            except Exception as e:results.append({'x':x,'y':y,'error':str(e)})
    return {'observations':results,'valid':sum('value_m' in x for x in results),'native_cells':0,'independent_accuracy_verified':False}
def main():
    report={}
    with ThreadPoolExecutor(max_workers=3) as pool:
        fs={k:pool.submit(fn) for k,fn in [('public_alias',alias),('wms_formats',wms),('wms_point_sample',point_check)]}
        for k,f in fs.items():
            try:report[k]={'status':'CHECKS_RECORDED','data':f.result()}
            except Exception as e:report[k]={'status':'FAILED','error':str(e)}
    report['native_cells_recovered']=report.get('public_alias',{}).get('data',{}).get('native_window',{}).get('cells',0)
    report['email_submitted']=False;report['production']='NO_GO';report['canonical_EAM_closed']=0
    (OUT/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    (OUT/'README.md').write_text('DM-13: font ICGC, MDT LiDAR AMB 25 cm 2022, CC BY 4.0. Les consultes WMS no equivalen a cel·les natives acreditades. Informació de procedència i comprovacions a report.json. NO-GO.\n')
    print(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False))
if __name__=='__main__':main()
