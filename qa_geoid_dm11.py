#!/usr/bin/env python3
"""Readback from custody; never re-download the three already-acquired sources."""
import json, hashlib, math, re, zlib, sys, urllib.request, urllib.parse
from pathlib import Path
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor
ROOT=Path('custody/dm11')
def require(x,m):
    if not x: raise ValueError(m)
def sha(b):return hashlib.sha256(b).hexdigest()
def data_check():
    checks={}
    for name,nb,hs in [('PENR2009.gsb',667552,'eb6bc0ca1de4591851d2113f44b6a16fb44ad67ab49dd7216ca471c41ce45221'),('ICGC_080193.json',458348,'c5605416b9e25b84eaaa6f9eb7a2bb8e15622644de2a40a799dcd1911be1078e'),('cat80000.gr',535816,None)]:
        b=(ROOT/name).read_bytes()
        digest=sha(b); crc=f'{zlib.crc32(b):08x}'
        require(len(b)==nb,'byte count mismatch '+name)
        if hs:require(digest==hs,'sha256 mismatch '+name)
        else:require(crc=='6b1c94a2','GR CRC differs from original DM-10 ZIP')
        checks[name]={'bytes':len(b),'sha256':digest,'crc32':crc,'git_blob_sha1':hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()}
    return checks
def geoid():
    b=(ROOT/'cat80000.gr').read_bytes()
    # Text format; header contains ISO-8859-1 characters. Do not rewrite the original.
    s=b.decode('latin1')
    h,sep,body=s.partition('\\DATA'); require(sep=='\\DATA','DATA marker absent')
    body,end,tail=body.partition('\\END OF DATA')
    require(end=='\\END OF DATA' and not tail.strip(),'unexpected GR terminator or trailer')
    def f(label):
        m=re.search(r'\\'+re.escape(label)+r'\s*=\s*([^\r\n]+)',h)
        require(m is not None,'header missing '+label); return m.group(1).strip()
    nr,nc=int(f('NUMBER OF ROWS')),int(f('NUMBER OF COLUMNS'))
    require((nr,nc)==(181,211),'unexpected dimensions')
    require(f('ITEM UNITS')=='m' and f('REFERENCE SYSTEM')=='ETRS89/00-EGM08D595','units/datum mismatch')
    require(f('LIST').split()==['0','0','.00','40','0','.00','0','1','.00','0','0','.00'],'orientation mismatch')
    require(f('UNITS')=='DEG DEG DEG DEG' and 'HELMERT' in h,'orientation type mismatch')
    vals=[float(x) for x in body.split()]
    require(len(vals)==nr*nc==int(f('NUMBER OF POINTS')),'node count')
    require(all(math.isfinite(x) for x in vals),'nonfinite value')
    require(min(vals)==float(f('MINIMUM'))==46.377 and max(vals)==float(f('MAXIMUM'))==53.938,'declared extrema mismatch')
    require(vals[0]==52.299,'first node differs from specification')
    def interp(lon,lat):
        x=lon*60;y=(43-lat)*60
        require(-1e-9<=x<=210+1e-9 and -1e-9<=y<=180+1e-9,'outside grid')
        x=max(0,min(x,210));y=max(0,min(y,180));i=min(int(y),179);j=min(int(x),209);u=x-j;v=y-i
        return (1-v)*((1-u)*vals[i*nc+j]+u*vals[i*nc+j+1])+v*((1-u)*vals[(i+1)*nc+j]+u*vals[(i+1)*nc+j+1])
    max_node_error=max(abs(interp(j/60,43-i/60)-vals[i*nc+j]) for i in range(nr) for j in range(nc))
    require(max_node_error<=1e-9,'grid node reproduction')
    # Published Leica/ICC integration example, p. 6, NOT independent observations.
    # The first three coordinates in the Leica screenshot differ from the archive input example.
    examples=[(1,0,39,38.86965,42,47,53.47289,53.0895),(2,0,1,0,42,59,0,52.4990),(3,0,2,0,42,59,0,52.5320),(4,0,58,32.403210,42,41,36.874910,53.7459),(5,0,45,1.930110,41,52,53.796350,49.8529),(6,2,54,15.022810,41,52,58.392820,49.4553),(7,1,54,50.544900,41,17,34.588910,49.3596),(8,1,58,22.990670,42,28,41.260980,52.8161),(9,2,25,43.891880,41,32,23.745830,49.2394),(10,1,59,13.043010,41,25,6.702080,49.3701),(11,1,10,6.712940,41,10,12.018780,49.1366),(12,2,10,27.472060,41,58,48.401470,50.4851),(13,1,7,57.915820,42,22,28.318840,52.4593),(14,0,35,46.669490,41,37,45.441810,49.4137),(15,1,31,.575410,41,59,32.058910,50.5288)]
    results=[]
    for ident,ld,lm,ls,ad,am,asec,ref in examples:
        lon=ld+lm/60+ls/3600;lat=ad+am/60+asec/3600; v=interp(lon,lat)
        results.append({'id':ident,'lon':lon,'lat':lat,'calculated_m':v,'published_m':ref,'delta_m':v-ref})
    return {'rows':nr,'columns':nc,'nodes':len(vals),'min_m':min(vals),'max_m':max(vals),'mean_m':math.fsum(vals)/len(vals),'corners_nw_ne_sw_se_m':[vals[0],vals[nc-1],vals[(nr-1)*nc],vals[-1]],'node_424_m':vals[423],'node_reproduction_max_abs_m':max_node_error,'bounds_lonlat':[0,40,3.5,43],'grid_step_arcmin':1,'reference_system':f('REFERENCE SYSTEM'),'declared_estimated_std_m':float(f('DEVCOR')),'reference_examples':results,'within_0_0001m':sum(abs(x['delta_m'])<=.0001 for x in results),'max_abs_m':max(abs(x['delta_m']) for x in results),'rmse_m':math.sqrt(math.fsum(x['delta_m']**2 for x in results)/len(results)),'reference_source':'https://icgc-web-pro.s3.eu-central-1.amazonaws.com/produccio/s3fs-public/INFO_cat80000%20to%20GEM_Leica%20Geoystems%20Geoid_21620.pdf p.6','independent_vertical_accuracy_verified':False}
class Page(HTMLParser):
    def __init__(self):super().__init__();self.scripts=[];self.elements=[]
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag=='script' and 'src' in a:self.scripts.append(a['src'])
        if tag in ('input','select','button','a') and any(re.search(r'met|elev|descarr|download|tif',str(v),re.I) for v in a.values()):self.elements.append([tag,a])
def get(url,cap=4*1024**2):
    req=urllib.request.Request(url,headers={'User-Agent':'Barcelona3D open data QA/DM11'})
    with urllib.request.urlopen(req,timeout=15) as r:
        b=r.read(cap+1);require(len(b)<=cap,'response exceeds cap')
        return b,r.status,r.geturl()
def viewer():
    url='https://visors.icgc.cat/appdownloads/?c=fmevtmet'
    raw,status,final=get(url); page=Page();page.feed(raw.decode('utf-8',errors='replace'))
    found=[];results=[]
    for src in page.scripts:
        u=urllib.parse.urljoin(final,src)
        if urllib.parse.urlparse(u).hostname not in ['visors.icgc.cat','geoserveis.icgc.cat','www.icgc.cat']:continue
        if len(results)>=12:break
        try:
            b,st,f=get(u);s=b.decode('utf-8',errors='replace')
            patterns=[r'fmevtmet',r'fmevtld',r'25cm.?2022',r'model-elevacions-terreny',r'GetCoverage',r'GetMap',r'downloadUrl',r'urlDownload',r'descarregaUrl']
            matches=[]
            for pat in patterns:
                for m in list(re.finditer(pat,s,re.I))[:3]:
                    matches.append({'term':pat,'context':s[max(0,m.start()-140):m.end()+180]})
            results.append({'url':f,'http':st,'sha256':sha(b),'matches':matches})
        except Exception as e:results.append({'url':u,'error':str(e)})
    return {'http':status,'url':final,'sha256':sha(raw),'elements':page.elements[:30],'scripts':page.scripts,'script_inspection':results,'native_cells_acquired':0}
def main():
    report={}
    for key,fn in [('custody_readback',data_check),('geoid',geoid),('viewer_alternate',viewer)]:
        try:report[key]={'status':'CHECKS_RECORDED','data':fn()}
        except Exception as e:report[key]={'status':'FAILED','error':str(e)}
    report['canonical_EAM_closed']=0;report['production']='NO_GO'
    p=ROOT/'qa-geoid-readback.json';p.write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    # Do not flood logs with HTML; report contains bounded snippets.
    print('DM11_READBACK_BEGIN'); print(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)); print('DM11_READBACK_END')
    return 0 if all(report[k]['status']=='CHECKS_RECORDED' for k in ('custody_readback','geoid')) else 1
if __name__=='__main__':sys.exit(main())
