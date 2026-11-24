#!/usr/bin/env python3
"""DM-12: bounded native COG window test after IPv4 range preflight.
No email submission, no full COG download, no synthetic raster or resampling.
"""
import hashlib,json,os,re,socket,subprocess,sys,math
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import requests
import rasterio
from rasterio.windows import Window
import numpy as np
import xml.etree.ElementTree as ET
URL='https://datacloud.icgc.cat/datacloud/model-elevacions-terreny/tif_unzip/model-elevacions-terreny-lidar-amb-25cm-2022-2022.tif'
WMS='https://geoserveis.icgc.cat/servei/catalunya/elevacions-territorial/wms'
LAYER='model-elevacions-terreny-amb-lidar-25cm-2022'
X,Y=429177.375,4582885.875
OUT=Path('custody/dm12');OUT.mkdir(parents=True,exist_ok=True)
TMP=Path('dm12-tmp');TMP.mkdir(exist_ok=True)
def require(b,msg):
    if not b:raise ValueError(msg)
def sha(b):return hashlib.sha256(b).hexdigest()
def write(name,b):
    p=OUT/name
    require(not p.exists() or p.read_bytes()==b,'immutable file collision')
    p.write_bytes(b)
    return {'path':str(p),'bytes':len(b),'sha256':sha(b)}
def curl_range():
    # New transport diagnostic vs. DM-10 requests: force IPv4, strict 64 KiB limit,
    # capture DNS/TLS/connect/HTTP timing and Content-Range. Never accept full body.
    ips=socket.getaddrinfo('datacloud.icgc.cat',443,type=socket.SOCK_STREAM)
    p=subprocess.run(['curl','--ipv4','--proto','=https','--proto-redir','=https','--location','--max-redirs','2','--connect-timeout','30','--max-time','45','--range','0-65535','--max-filesize','65536','--header','Accept-Encoding: identity','--dump-header',str(TMP/'headers.txt'),'--output',str(TMP/'prefix.bin'),'--silent','--show-error','--write-out','%{http_code} %{remote_ip} %{time_namelookup} %{time_connect} %{time_appconnect} %{size_download}',URL],capture_output=True,text=True,timeout=50)
    result={'dns':sorted({a[4][0] for a in ips}),'curl_exit':p.returncode,'curl_metrics':p.stdout,'stderr':p.stderr[:1000]}
    header=(TMP/'headers.txt').read_text(errors='replace') if (TMP/'headers.txt').exists() else ''
    result['headers']=header[:4000]
    raw=(TMP/'prefix.bin').read_bytes() if (TMP/'prefix.bin').exists() else b''
    result['received_bytes']=len(raw)
    match=re.findall(r'^Content-Range:\s*bytes 0-65535/(\d+)\s*$',header,re.I|re.M)
    result['range_accepted']=p.returncode==0 and p.stdout.startswith('206 ') and len(raw)==65536 and match==['24310268511'] and raw[:4] in (b'II*\0',b'MM\0*',b'II+\0',b'MM\0+')
    if result['range_accepted']:result['header_sha256']=sha(raw)
    return result
def wms_point(x,y):
    p={'service':'WMS','version':'1.3.0','request':'GetFeatureInfo','layers':LAYER,'query_layers':LAYER,'styles':'','crs':'EPSG:25831','bbox':f'{x-.125},{y-.125},{x+.125},{y+.125}','width':1,'height':1,'i':0,'j':0,'format':'image/png','info_format':'application/json'}
    r=requests.get(WMS,params=p,timeout=(10,20));r.raise_for_status()
    j=r.json();require(j.get('name')==LAYER,'unexpected WMS layer')
    f=j['features'];require(len(f)==1,'unexpected WMS feature count')
    xx,yy=f[0]['geometry']['coordinates'];require(abs(xx-x)<1e-6 and abs(yy-y)<1e-6,'WMS position mismatch')
    v=float(f[0]['properties']['Elevació']);require(math.isfinite(v),'nonfinite WMS')
    return {'x':xx,'y':yy,'elevation_m':v,'url':r.url,'sha256':sha(r.content)}
def native():
    # GDAL exact source windows (no out_shape, resampling or reprojection).
    with rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR',CPL_VSIL_CURL_ALLOWED_EXTENSIONS='.tif',GDAL_HTTP_TIMEOUT='20',GDAL_HTTP_CONNECTTIMEOUT='15',GDAL_HTTP_MAX_RETRY='0',GDAL_HTTP_MULTIRANGE='SERIAL',GDAL_INGESTED_BYTES_AT_OPEN='65536'):
        with rasterio.open('/vsicurl/'+URL) as ds:
            require(ds.crs and ds.crs.to_epsg()==25831,'unexpected horizontal CRS')
            require(ds.count==1 and ds.dtypes[0]=='float32','not one-band Float32')
            require(ds.transform.a==.25 and ds.transform.e==-.25 and ds.transform.b==0 and ds.transform.d==0,'unexpected native grid')
            require(ds.nodata==-9999,'NoData differs from specification')
            row,col=ds.index(X,Y)
            cx,cy=ds.xy(row,col)
            require(abs(cx-X)<1e-8 and abs(cy-Y)<1e-8,'WMS point not aligned to native pixel center')
            out=[]
            for name,r,c in [('interior_1',row-32,col-32),('interior_2',row-32,col+32)]:
                win=Window(c,r,64,64)
                require(r>=0 and c>=0 and r+64<=ds.height and c+64<=ds.width,'window outside source')
                a=ds.read(1,window=win,masked=True); valid=a.compressed()
                require(a.shape==(64,64) and valid.size==4096 and np.isfinite(valid).all(),'window incomplete/nonfinite')
                path=TMP/(name+'.tif')
                profile={'driver':'GTiff','width':64,'height':64,'count':1,'dtype':'float32','crs':ds.crs,'transform':ds.window_transform(win),'nodata':ds.nodata,'compress':'deflate'}
                with rasterio.open(path,'w',**profile) as dst:dst.write(a.data,1)
                with rasterio.open(path) as chk:
                    require(np.array_equal(chk.read(1).view('uint32'),a.data.view('uint32')) and chk.transform==profile['transform'] and chk.crs==ds.crs,'native window write/read mismatch')
                q=write(name+'.tif',path.read_bytes());q.update(window=[c,r,64,64],valid=int(valid.size),min_m=float(valid.min()),max_m=float(valid.max()),mean_m=float(valid.mean()),bounds=list(rasterio.windows.bounds(win,ds.transform)),native_values_unchanged=True)
                out.append(q)
            value=float(ds.read(1,window=Window(col,row,1,1))[0,0]);obs=wms_point(cx,cy)
            return {'crs':ds.crs.to_string(),'dtype':ds.dtypes[0],'nodata':ds.nodata,'source_shape':[ds.height,ds.width],'transform':list(ds.transform),'block_shapes':ds.block_shapes,'windows':out,'valid_native_cells':8192,'wms_comparison':{'native_m':value,'wms':obs,'delta_m':value-obs['elevation_m']},'independent_vertical_accuracy_verified':False}
def services():
    # Distinct public official interfaces, not substitutes for original cells.
    urls={'territorial_elevations':'https://api.icgc.cat/territorial/elements/elevacions/429177.375,4582885.875','territorial_processes':'https://api.icgc.cat/territorial/processes','legacy_wcs':'https://geoserveis.icgc.cat/icc_mdt/wcs/service?SERVICE=WCS&REQUEST=GetCapabilities&VERSION=1.0.0'}
    def one(q):
        name,url=q
        try:
            with requests.get(url,timeout=(10,20),stream=True) as r:
                b=r.raw.read(1024*1024+1,decode_content=True);require(len(b)<=1024*1024,'response exceeds cap')
                ret={'http':r.status_code,'url':r.url,'sha256':sha(b),'body_excerpt':b.decode('utf-8',errors='replace')[:3000]}
                if name=='legacy_wcs':
                    try:
                        root=ET.fromstring(b);ret['coverage_names']=[e.text for e in root.iter() if e.tag.split('}')[-1] in ('name','label','CoverageId')][:50]
                    except ET.ParseError:pass
                return name,ret
        except Exception as e:return name,{'error':str(e)}
    with ThreadPoolExecutor(max_workers=3) as pool:return dict(pool.map(one,urls.items()))
def main():
    report={}
    with ThreadPoolExecutor(max_workers=2) as ex:
        s=ex.submit(services)
        try:
            report['ipv4_range']=curl_range()
            if report['ipv4_range']['range_accepted']:
                try:report['native']={'status':'CHECKS_RECORDED','data':native()}
                except Exception as e:report['native']={'status':'FAILED','error':str(e)}
            else:report['native']={'status':'NOT_EXECUTED','reason':'bounded transport preflight did not establish valid HTTP range'}
        except Exception as e:report['ipv4_range']={'error':str(e)};report['native']={'status':'NOT_EXECUTED'}
        report['alternate_services']=s.result()
    report['native_cells_recovered']=report.get('native',{}).get('data',{}).get('valid_native_cells',0)
    report['independent_vertical_accuracy_verified']=False
    report['canonical_EAM_closed']=0;report['production']='NO_GO';report['email_submitted']=False
    (OUT/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    (OUT/'README.md').write_text('DM-12. Font: Institut Cartogràfic i Geològic de Catalunya, MDT LiDAR AMB 25 cm 2022, CC BY 4.0 https://creativecommons.org/licenses/by/4.0/. Les finestres eventualment presents són retalls de valors nadius sense resampling, no el COG complet. Dades i procedència a report.json. NO-GO.\n')
    print(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False))
if __name__=='__main__':main()
