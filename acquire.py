#!/usr/bin/env python3
"""Bounded, read-only acquisition of public Barcelona geospatial source data.
No external writes, no paid APIs; errors never count as accepted evidence.
"""
import argparse, hashlib, json, os, struct, sys, zipfile, re
from pathlib import Path
import numpy as np
import requests
import rasterio
from rasterio.windows import Window
from pyproj import CRS
from shapely.geometry import Polygon, MultiPolygon
from shapely import geos_version_string
import shapefile

PEN = ['https://datos-geodesia.ign.es/Utilidades/Rejillas_ed50-etrs89/GDS-TOOLS-PENR2009.zip', 'https://www.ign.es/web/resources/docs/IGNCnig/GDS-TOOLS-PENR2009.zip']
GEO = 'https://icgc-web-pro.s3.eu-central-1.amazonaws.com/produccio/s3fs-public/EGM08D595_19839.zip'
POLY = 'https://maps.icgc.cat/vector01/rest/services/divisions_administratives_wms/MapServer/12/query'
MDT = 'https://datacloud.icgc.cat/datacloud/model-elevacions-terreny/tif_unzip/model-elevacions-terreny-lidar-amb-25cm-2022-2022.tif'
SPED = 'https://raw.githubusercontent.com/oscarfonts/geocalc/master/src/main/resources/org/geotools/referencing/factory/gridshift/SPED2ETV2.gsb'
HIST = 'https://raw.githubusercontent.com/martgnz/bcn-geodata/master/src/0301040100_TermeMunicipal_UNITATS_ADM'
HIST_SHA = {'.shp':'1fd34b4e6afb1da79ed4c14bf421e85c70783295','.shx':'26b9312b066694ee4516a5316d8fde2edd621c8f','.dbf':'171ceb059b2ad30d971a0afd1fd8912d13b2efc5','.prj':'382d16800977c1aaeaa00e9d67264221d97d1db1'}

def require(test, msg):
    if not test: raise ValueError(msg)

def gitsha(b): return hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
def sha(b): return hashlib.sha256(b).hexdigest()

def download(url, dest, cap, params=None):
    dest=Path(dest); temp=dest.with_suffix(dest.suffix+'.part')
    try:
        with requests.get(url,params=params,timeout=(15,45),stream=True) as r:
            r.raise_for_status(); require(r.url.startswith('https://'),'redirect not HTTPS')
            if r.headers.get('Content-Length'): require(int(r.headers['Content-Length'])<=cap,'declared length exceeds cap')
            n=0
            with temp.open('wb') as f:
                for block in r.iter_content(65536):
                    n+=len(block); require(n<=cap,'download cap exceeded'); f.write(block)
            require(n>0,'empty response'); temp.replace(dest)
            return {'url':r.url,'bytes':n,'sha256':sha(dest.read_bytes()),'http':r.status_code,'content_type':r.headers.get('Content-Type')}
    finally:
        temp.unlink(missing_ok=True)

def zip_inventory(path):
    with zipfile.ZipFile(path) as z:
        require(sum(i.file_size for i in z.infolist())<=128*1024**2,'ZIP inflated size exceeds cap')
        require(z.testzip() is None,'ZIP CRC failed')
        return [{'path':i.filename,'bytes':i.file_size,'crc32':f'{i.CRC:08x}'} for i in z.infolist() if not i.is_dir()]

def ntv2(b):
    require(b[:8]==b'NUM_OREC','not NTv2')
    endian=next((e for e in ('<','>') if struct.unpack_from(e+'i',b,8)[0]==11),None)
    require(endian is not None,'unknown byte order')
    I=lambda p:struct.unpack_from(endian+'i',b,p)[0]
    require(I(24)==11,'unexpected subheader count'); count=I(40); require(0<count<100,'invalid grid count')
    o=176; result={}
    for _ in range(count):
        require(o+176<=len(b),'truncated header')
        fields={b[i:i+8].decode('ascii').strip():b[i+8:i+16] for i in range(o,o+176,16)}
        D=lambda k:struct.unpack(endian+'d',fields[k])[0]
        name=fields['SUB_NAME'].decode('ascii').strip(); n=I(o+168)
        bounds=[D(k) for k in ('S_LAT','N_LAT','E_LONG','W_LONG','LAT_INC','LONG_INC')]
        s,north,e,w,dy,dx=bounds; require(dy>0 and dx>0 and north>=s and w>=e,'bad extent')
        rows=round((north-s)/dy)+1; cols=round((w-e)/dx)+1
        require(rows*cols==n and n>0 and name not in result,'grid dimensions mismatch')
        o+=176; require(o+16*n<=len(b),'truncated data')
        a=np.frombuffer(b,dtype=endian+'f4',count=4*n,offset=o).reshape(rows,cols,4).astype(np.float32)
        require(np.isfinite(a[:,:,:2]).all(),'nonfinite offset'); o+=16*n
        result[name]=(bounds,a)
    require(b[o:o+8]==b'END     ' and o+16==len(b),'missing end/trailing bytes')
    return result

def compare_ntv2(a,b):
    require(a[0]==b[0] and a[1].shape==b[1].shape,'grid registration differs')
    x,y=a[1][:,:,:2],b[1][:,:,:2]
    return {'nodes':x.shape[0]*x.shape[1],'offset_count':x.size,'bit_equal':int(np.count_nonzero(x.view('uint32')==y.view('uint32'))),'max_delta_arcsec':float(np.max(np.abs(x-y)))}

def pen_task(out):
    attempts=[]; path=out/'PENR2009.zip'
    for url in PEN:
        try:
            evidence=download(url,path,64*1024**2); members=zip_inventory(path); break
        except Exception as e: attempts.append({'url':url,'error':str(e)})
    else: raise ValueError(json.dumps(attempts,ensure_ascii=False))
    # File name is not sufficient: inspect the actual NTv2 content.
    refpath=out/'SPED2ETV2.gsb'; refmeta=download(SPED,refpath,4*1024**2); refbytes=refpath.read_bytes()
    require(gitsha(refbytes)=='4bfdd1937f801ce9dd3bdb5e5d003f522dc77d4c','reference Git hash differs from DM-08')
    ref=ntv2(refbytes)['PENINSUL']; candidates=[]
    with zipfile.ZipFile(path) as z:
        for i in z.infolist():
            if i.is_dir() or i.file_size>32*1024**2: continue
            with z.open(i) as f: head=f.read(8)
            if head!=b'NUM_OREC': continue
            raw=z.read(i); grids=ntv2(raw)
            for name,grid in grids.items():
                if grid[0]==ref[0] and grid[1].shape==ref[1].shape:
                    result=compare_ntv2(grid,ref); result.update(member=i.filename,grid=name,sha256=sha(raw)); candidates.append(result)
    require(len(candidates)>0,'no matching peninsular grid registration')
    return {'download':evidence,'members':members,'reference':refmeta,'attempts':attempts,'comparisons':candidates,'all_offsets_equal':all(c['bit_equal']==c['offset_count'] for c in candidates)}

def arc_polygon(rings):
    outer=[]; holes=[]
    for ring in rings:
        require(len(ring)>=4 and ring[0]==ring[-1],'unclosed ring')
        p=Polygon(ring); require(p.is_valid and not p.is_empty and p.area>0,'invalid input ring')
        (holes if p.exterior.is_ccw else outer).append((ring,p))
    require(outer,'no outer rings'); assigned=[[] for _ in outer]
    for ring,p in holes:
        owners=[i for i,(_,q) in enumerate(outer) if q.contains(p)]
        require(len(owners)==1,'ambiguous hole'); assigned[owners[0]].append(ring)
    result=MultiPolygon([Polygon(ring,assigned[i]) for i,(ring,_) in enumerate(outer)])
    require(result.is_valid and not result.is_empty,'invalid polygon topology'); return result

def polygon_task(out):
    q={'where':"CODIMUNI='080193'",'outFields':'*','returnGeometry':'true','outSR':25831,'f':'json'}
    p=out/'ICGC_080193.json'; meta=download(POLY,p,32*1024**2,q); j=json.loads(p.read_bytes())
    require('error' not in j and not j.get('exceededTransferLimit',False),'ArcGIS error or truncation')
    sr=j.get('spatialReference',{}); require(25831 in (sr.get('wkid'),sr.get('latestWkid')),'unexpected official CRS')
    fs=j['features']; require(len(fs)==1,'expected single municipality record')
    attrs=fs[0]['attributes']; require(str(attrs.get('CODIMUNI'))=='080193','municipality mismatch')
    official=arc_polygon(fs[0]['geometry']['rings'])
    for ext,expected in HIST_SHA.items():
        path=out/('historic'+ext); download(HIST+ext,path,4*1024**2); require(gitsha(path.read_bytes())==expected,'historical file changed '+ext)
    require(CRS.from_wkt((out/'historic.prj').read_text()).equals(CRS.from_epsg(25831),ignore_axis_order=True),'historical CRS mismatch')
    with shapefile.Reader(str(out/'historic.shp')) as reader:
        require(len(reader)==1,'historical record count'); s=reader.shape(0); idx=list(s.parts)+[len(s.points)]; historic=arc_polygon([s.points[idx[i]:idx[i+1]] for i in range(len(idx)-1)])
    return {'download':meta,'attributes':attrs,'geos':geos_version_string,'official_valid':official.is_valid,'historic_valid':historic.is_valid,'official_area_m2':official.area,'historic_area_m2':historic.area,'official_perimeter_m':official.length,'historic_perimeter_m':historic.length,'intersection_m2':official.intersection(historic).area,'symmetric_difference_m2':official.symmetric_difference(historic).area,'hausdorff_m':official.hausdorff_distance(historic),'official_bounds':official.bounds,'official_parts':len(official.geoms),'historic_parts':len(historic.geoms),'representative_point':[official.representative_point().x,official.representative_point().y]}

def geoid_task(out):
    path=out/'EGM08D595.zip'; evidence=download(GEO,path,64*1024**2); members=zip_inventory(path)
    # Acquisition and archive integrity only. No guessing a proprietary format or vertical datum.
    return {'download':evidence,'members':members,'vertical_values_verified':0,'vertical_reference_homologated':False}

def mdt_task(out,polygon=None):
    with requests.get(MDT,headers={'Range':'bytes=0-16383','Accept-Encoding':'identity'},timeout=(15,45),stream=True) as r:
        require(r.status_code==206,'server did not return HTTP 206; full download prohibited')
        cr=r.headers.get('Content-Range',''); require(re.fullmatch(r'bytes 0-16383/\d+',cr) is not None,'invalid range response')
        head=r.raw.read(16385); require(len(head)==16384 and head[:4] in (b'II*\x00',b'MM\x00*',b'II+\x00',b'MM\x00+'),'invalid TIFF header')
    (out/'MDT_header.bin').write_bytes(head)
    # Real IGN ETRS89 points from DM-07; these are window locations, NOT accepted ground controls.
    positions=[('Poblenou',434323.113,4584631.149),('Puig_Castellar',433749.805,4591272.188)]
    if polygon: positions.append(('interior_municipal',*polygon['representative_point']))
    result=[]
    with rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR',CPL_VSIL_CURL_ALLOWED_EXTENSIONS='.tif',GDAL_HTTP_TIMEOUT='30',GDAL_HTTP_MAX_RETRY='0',GDAL_HTTP_MULTIRANGE='SERIAL'):
        with rasterio.open('/vsicurl/'+MDT) as ds:
            require(ds.crs is not None and ds.crs.to_epsg()==25831,'unexpected horizontal CRS')
            require(abs(ds.res[0]-.25)<1e-8 and abs(ds.res[1]-.25)<1e-8 and ds.count==1,'not native 25 cm one-band raster')
            for name,x,y in positions:
                row,col=ds.index(x,y); win=Window(col-32,row-32,64,64)
                if win.col_off<0 or win.row_off<0 or win.col_off+64>ds.width or win.row_off+64>ds.height:
                    result.append({'name':name,'status':'OUT_OF_EXTENT'}); continue
                a=ds.read(1,window=win,masked=True); valid=a.compressed(); require(a.shape==(64,64),'wrong native window shape')
                require(np.isfinite(valid).all(),'nonfinite valid terrain cells')
                path=out/(name+'.tif'); profile=ds.profile.copy(); profile.update(driver='GTiff',width=64,height=64,count=1,transform=ds.window_transform(win),tiled=False,compress='deflate'); profile.pop('blockxsize',None); profile.pop('blockysize',None)
                with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
                    with rasterio.open(path,'w',**profile) as dst: dst.write(a.data,1); dst.write_mask((~np.ma.getmaskarray(a)).astype('uint8')*255)
                result.append({'name':name,'native_window':list(win.flatten()),'shape':list(a.shape),'valid_cells':int(valid.size),'min_m':float(valid.min()) if valid.size else None,'max_m':float(valid.max()) if valid.size else None,'sha256':sha(path.read_bytes())})
            return {'content_range':cr,'header_sha256':sha(head),'size':[ds.width,ds.height],'crs':ds.crs.to_wkt(),'resolution':ds.res,'tags':ds.tags(),'windows':result,'independent_vertical_accuracy_verified':False}

def offline(gsb,tif):
    raw=Path(gsb).read_bytes(); grids=ntv2(raw); bounds,a=grids['0INT2GRS']; expected=np.stack([a[::-1,::-1,0],-a[::-1,::-1,1]])
    with rasterio.open(tif) as ds:
        actual=ds.read(); require(actual.shape==expected.shape,'shape mismatch'); require(ds.crs.to_epsg()==4230 and ds.tags()['target_crs_epsg_code']=='4258','CRS mismatch')
        require(np.array_equal(actual.view('uint32'),expected.view('uint32')),'offset mismatch')
    return {'real_source_gsb_sha256':sha(raw),'real_source_tif_sha256':sha(Path(tif).read_bytes()),'nodes':int(a.shape[0]*a.shape[1]),'bitwise_equal_offsets':int(actual.size),'status':'PASS_LOCAL_REAL_DATA_ONLY'}

def main():
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,default=Path('evidence'));p.add_argument('--offline',nargs=2);args=p.parse_args()
    if args.offline: print(json.dumps(offline(*args.offline),indent=2));return 0
    if os.environ.get('GITHUB_ACTIONS')=='true': require(os.environ.get('REPOSITORY_VISIBILITY')=='public','refusing to run in nonpublic repository')
    args.out.mkdir(parents=True,exist_ok=True); results={}
    for name,fn in [('PENR2009',lambda:pen_task(args.out)),('polygon',lambda:polygon_task(args.out)),('EGM08D595',lambda:geoid_task(args.out)),('MDT',lambda:mdt_task(args.out,results.get('polygon',{}).get('data')))]:
        try: results[name]={'status':'ACQUIRED_CHECKS_RECORDED_NOT_HOMOLOGATED','data':fn()}
        except Exception as e: results[name]={'status':'FAILED','error':str(e)}
    results['canonical_EAM_closed']=0;results['production']='NO_GO'; report=json.dumps(results,ensure_ascii=False,indent=2,allow_nan=False); (args.out/'report.json').write_text(report); print(report)
    return 1 if any(v.get('status')=='FAILED' for v in results.values() if isinstance(v,dict)) else 0
if __name__=='__main__': sys.exit(main())
