#!/usr/bin/env python3
"""DM-11 custody: data only, preserve originals, no EXE execution.
ICGC / IGN attribution in custody/README.md. No paid storage, caches or APIs.
"""
from pathlib import Path
import acquire as ac
import hashlib, zipfile, json, re, subprocess, math, sys
import numpy as np
from pypdf import PdfReader
from io import BytesIO
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

OUT=Path('custody/dm11')
TMP=Path('dm11-tmp')
OUT.mkdir(parents=True,exist_ok=True)
TMP.mkdir(exist_ok=True)
EXPECTED={
'PENR2009.zip':'6a507bc02ad09eaeb4ba71c612d4b8e160ce2ae2b2c0f3611e93ea20c2192945',
'ICGC_080193.json':'c5605416b9e25b84eaaa6f9eb7a2bb8e15622644de2a40a799dcd1911be1078e',
'EGM08D595.zip':'fa33fdebeb70169035a64771530f39ea05b883b4dc55c639134edb03f9b4c432'}
REPORT={}
def req(x,m):
    if not x: raise ValueError(m)
def save(name,b):
    p=OUT/name
    req(not p.exists() or p.read_bytes()==b,'immutable custody collision: '+name)
    p.write_bytes(b)
    return {'path':str(p),'bytes':len(b),'sha256':ac.sha(b)}
def dl(name,url,params=None):
    p=TMP/name; meta=ac.download(url,p,16*1024**2,params)
    req(meta['sha256']==EXPECTED[name],'DM-10 hash mismatch: '+name)
    return p,meta
def pdftext(z,member):
    return '\n'.join(page.extract_text() or '' for page in PdfReader(BytesIO(z.read(member))).pages)
def pen():
    p,meta=dl('PENR2009.zip',ac.PEN[0]); ac.zip_inventory(p)
    with zipfile.ZipFile(p) as z:
        lic=pdftext(z,'Licencia_License.pdf')+'\n'+pdftext(z,'Condiciones_Licencia_License_Conditions.pdf')
        relevant=[l.strip() for l in lic.splitlines() if re.search(r'CC.BY|Creative Commons|reutiliza|distribuci|atribuci|reconocimiento|licencia.*4\.0',l,re.I)]
        print('PENR_LICENSE_EVIDENCE',json.dumps(relevant[:20],ensure_ascii=False),flush=True)
        req(re.search(r'CC[\s-]*BY[\s-]*4\.0|creativecommons\.org/licenses/by/4\.0|Creative Commons.{0,100}4\.0',lic,re.I|re.S),'specific license CC BY 4.0 not detected; no publication')
        b=z.read('PENR2009.gsb')
    req(ac.sha(b)=='eb6bc0ca1de4591851d2113f44b6a16fb44ad67ab49dd7216ca471c41ce45221','GSB hash mismatch')
    grids=ac.ntv2(b); return {'archive':meta,'data':save('PENR2009.gsb',b),'nodes':sum(v[1].shape[0]*v[1].shape[1] for v in grids.values()),'license_lines':relevant[:8]}
def polygon():
    p,meta=dl('ICGC_080193.json',ac.POLY,{'where':"CODIMUNI='080193'",'outFields':'*','returnGeometry':'true','outSR':25831,'f':'json'})
    j=json.loads(p.read_bytes()); req(len(j['features'])==1 and j['features'][0]['attributes']['CODIMUNI']=='080193','municipality')
    geom=ac.arc_polygon(j['features'][0]['geometry']['rings'])
    req(geom.is_valid,'GEOS invalid')
    return {'download':meta,'data':save('ICGC_080193.json',p.read_bytes()),'area_m2':geom.area,'parts':len(geom.geoms)}
def geoid():
    p,meta=dl('EGM08D595.zip',ac.GEO); ac.zip_inventory(p)
    with zipfile.ZipFile(p) as z:
        spec=pdftext(z,'Format_geoide_v2.pdf')
        lines=[l.strip() for l in spec.splitlines() if re.search(r'fila|columna|interpol|latitud|longitud|minut|HELMERT|orientaci|ondulaci|\.gr|ETRS89',l,re.I)]
        print('GEOID_SPEC_EVIDENCE',json.dumps(lines[:60],ensure_ascii=False),flush=True)
        b=z.read('EGM08D595/cat80000.gr')
        input_text=z.read('EGM08D595/input_GEO_eh.dat').decode('latin1')
        readme=z.read('EGM08D595/llegiu.me').decode('latin1')
        print('GEOID_README',readme[:2000],flush=True)
        print('GEOID_SAMPLE_INPUT',input_text[:1600],flush=True)
    req(len(b)==535816,'unexpected GR length')
    saved=save('cat80000.gr',b)
    txt=b.decode('latin1',errors='strict')
    h,sep,data=txt.partition('\\DATA')
    req(sep and '\\GRID SET' in h,'GR structure')
    print('GEOID_HEADER',h,flush=True)
    def f(label): 
        m=re.search(r'\\'+re.escape(label)+r'\s*=\s*([^\r\n]+)',h)
        req(m is not None,'header missing '+label);return m.group(1).strip()
    nr,nc=int(f('NUMBER OF ROWS')),int(f('NUMBER OF COLUMNS'))
    req((nr,nc)==(181,211),'grid dimensions')
    req(f('ITEM UNITS')=='m' and 'ETRS89/00-EGM08D595' in f('REFERENCE SYSTEM'),'datum or unit')
    req('HELMERT' in h and 'DEG DEG DEG DEG' in h,'grid orientation')
    req(re.search(r'\\LIST\s*=\s*0\s+0\s+\.00\s+40\s+0\s+\.00\s+0\s+1\s+\.00\s+0\s+0\s+\.00',h) is not None,'orientation params not expected')
    vals=np.array([float(x) for x in data.split()],dtype=np.float64)
    req(vals.size==nr*nc and np.isfinite(vals).all(),'data count or nonfinite')
    req(abs(vals.min()-float(f('MINIMUM')))<1e-9 and abs(vals.max()-float(f('MAXIMUM')))<1e-9,'declared min max mismatch')
    a=vals.reshape(nr,nc)
    def interp(lon,lat):
        x=lon*60;y=(43-lat)*60
        req(0<=x<=nc-1 and 0<=y<=nr-1,'outside grid')
        i=min(int(y),nr-2);j=min(int(x),nc-2);u=x-j;v=y-i
        return float((1-v)*((1-u)*a[i,j]+u*a[i,j+1])+v*((1-u)*a[i+1,j]+u*a[i+1,j+1]))
    # Numeric example published in Leica document hosted by ICGC (p.6), not an independent GNSS observation.
    examples=[(1,0,39,38.86965,42,47,53.47289,53.0895),(2,0,1,0,42,59,0,52.4990),(3,0,2,0,42,59,0,52.5320),(4,0,58,32.403210,42,41,36.874910,53.7459),(5,0,45,1.930110,41,52,53.796350,49.8529),(6,2,54,15.022810,41,52,58.392820,49.4553),(7,1,54,50.544900,41,17,34.588910,49.3596),(8,1,58,22.990670,42,28,41.260980,52.8161),(9,2,25,43.891880,41,32,23.745830,49.2394),(10,1,59,13.043010,41,25,6.702080,49.3701),(11,1,10,6.712940,41,10,12.018780,49.1366),(12,2,10,27.472060,41,58,48.401470,50.4851),(13,1,7,57.915820,42,22,28.318840,52.4593),(14,0,35,46.669490,41,37,45.441810,49.4137),(15,1,31,.575410,41,59,32.058910,50.5288)]
    cmp=[]
    for id,ld,lm,ls,ad,am,asec,ref in examples:
        lon=ld+lm/60+ls/3600;lat=ad+am/60+asec/3600
        v=interp(lon,lat);cmp.append({'id':id,'value_m':v,'reference_m':ref,'delta_m':v-ref})
    result={'archive':meta,'data':saved,'dimensions':[nr,nc],'nodes':int(vals.size),'range_m':[float(vals.min()),float(vals.max())],'mean_m':float(vals.mean()),'corner_m':[float(a[0,0]),float(a[0,-1]),float(a[-1,0]),float(a[-1,-1])],'reference_checks':cmp,'max_abs_reference_delta_m':max(abs(q['delta_m']) for q in cmp),'reference_within_0_0001m':sum(abs(q['delta_m'])<=.0001 for q in cmp),'independent_vertical_accuracy_verified':False}
    return result
def mdt_probe():
    # This is an alternate documented WMS endpoint, not a repeat of the failed COG request.
    url='https://geoserveis.icgc.cat/servei/catalunya/elevacions-territorial/wms'
    r=requests.get(url,params={'service':'WMS','request':'GetCapabilities','version':'1.3.0'},timeout=(15,25))
    r.raise_for_status(); req(len(r.content)<8*1024**2,'capabilities too big')
    root=ET.fromstring(r.content); layer='model-elevacions-terreny-amb-lidar-25cm-2022'
    req(any(e.text==layer for e in root.iter() if e.tag.endswith('}Name') or e.tag=='Name'),'layer absent')
    formats=[]
    for el in root.iter():
        if el.tag.split('}')[-1]=='GetFeatureInfo': formats=[e.text for e in el if e.tag.split('}')[-1]=='Format']
    result={'capabilities_http':r.status_code,'capabilities_sha256':ac.sha(r.content),'layer':layer,'feature_info_formats':formats,'native_cells_recovered':0}
    # Numerical GetFeatureInfo is a distinct observation; never certify a native COG cell.
    fmt=next((f for f in ('application/json','text/plain','text/xml','application/vnd.ogc.gml','text/html') if f in formats),None)
    if fmt:
        q={'service':'WMS','version':'1.3.0','request':'GetFeatureInfo','layers':layer,'query_layers':layer,'styles':'','crs':'EPSG:25831','bbox':'429169.875,4582877.375,429185.875,4582893.375','width':64,'height':64,'i':32,'j':32,'format':'image/png','info_format':fmt}
        t=requests.get(url,params=q,timeout=(15,25));result['feature_info']={'http':t.status_code,'content_type':t.headers.get('content-type'),'body':t.text[:2000]}
    # Discover whether the same endpoint actually implements WCS; candidate, not an assertion.
    wcs='https://geoserveis.icgc.cat/servei/catalunya/elevacions-territorial/wcs'
    try:
        q=requests.get(wcs,params={'service':'WCS','request':'GetCapabilities','version':'2.0.1'},timeout=(10,20));result['wcs_candidate']={'http':q.status_code,'content_type':q.headers.get('content-type'),'native_layer_name_present':layer in q.text,'head':q.text[:300]}
    except Exception as e:result['wcs_candidate']={'error':str(e)}
    return result
def main():
    with ThreadPoolExecutor(max_workers=4) as pool:
        fs={k:pool.submit(fn) for k,fn in [('PENR2009',pen),('polygon',polygon),('EGM08D595',geoid),('MDT_alternate',mdt_probe)]}
        for k,f in fs.items():
            try: REPORT[k]={'status':'CHECKS_RECORDED','data':f.result()}
            except Exception as e: REPORT[k]={'status':'FAILED','error':str(e)}
    REPORT['canonical_EAM_closed']=0;REPORT['production']='NO_GO'
    print('DM11_REPORT_BEGIN',flush=True);print(json.dumps(REPORT,ensure_ascii=False,indent=2,allow_nan=False),flush=True);print('DM11_REPORT_END',flush=True)
    (OUT/'report.json').write_text(json.dumps(REPORT,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    (OUT/'README.md').write_text('## Custodia DM-11\nDades originals sense modificació. PENR2009: Instituto Geográfico Nacional de España, CC BY 4.0 (https://www.ign.es/web/eu/politica-datos). Polígon municipal i EGM08D595: Institut Cartogràfic i Geològic de Catalunya (ICGC), CC BY 4.0 (https://www.icgc.cat/ca/LICGC/Informacio-publica/Transparencia/Reutilitzacio-de-la-informacio). Fonts i hashes a report.json. No es redistribueixen els EXE ni els arxius ZIP complets. Llicència https://creativecommons.org/licenses/by/4.0/. Dades de DM-10, revalidació per SHA-256. NO-GO.\n')
if __name__=='__main__':main()
