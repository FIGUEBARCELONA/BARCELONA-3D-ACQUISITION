import urllib.request,urllib.parse,re,json,hashlib
from html.parser import HTMLParser
class Form(HTMLParser):
    def __init__(self):super().__init__();self.form=[];self.fields=[]
    def handle_starttag(self,t,a):
        a=dict(a)
        if t=='form':self.form.append({k:v for k,v in a.items() if k in ('action','method','id')})
        if t in ('input','button','select','textarea'):self.fields.append({k:v for k,v in a.items() if k in ('type','name','id','required','placeholder','autocomplete')})
def get(url,limit=2*1024*1024):
    req=urllib.request.Request(url,headers={'User-Agent':'Barcelona3D open-data QA'})
    with urllib.request.urlopen(req,timeout=20) as r:
        b=r.read(limit+1)
        if len(b)>limit:raise ValueError('oversize')
        return b,r.geturl(),r.status
out={}
try:
    urls=['https://visors.icgc.cat/appdownloads/lib/params.js','https://visors.icgc.cat/appdownloads/lib/downloads300926.js','https://visors.icgc.cat/appdownloads/lib/customFunctions300926.js']
    scripts=[]
    for u in urls:
        b,_,st=get(u);s=b.decode('utf-8',errors='replace');scripts.append(s)
    ss='\n'.join(scripts)
    candidates=re.findall(r'(?:var|let|const)\s+zipserverNode\s*=\s*["\'](https://[^"\']+)["\']',ss)
    if not candidates:
        candidates=re.findall(r'zipserverNode\s*=\s*["\'](https://[^"\']+)["\']',ss)
    out['public_base_candidates']=list(dict.fromkeys(candidates))
    out['area_formula_context']=[s[max(0,m.start()-80):m.end()+120] for s in scripts[1:] for m in list(re.finditer(r'(?:var|let|const)\s+userha\s*=',s))[:3]]
    if len(set(candidates))==1:
        base=candidates[0].rstrip('/');host=urllib.parse.urlparse(base).hostname or ''
        if not host.endswith('.icgc.cat'):raise ValueError('unexpected public host')
        # This is the public UI form URL, not the server-side submission. 64 m square, 0.4096 ha.
        url=base+'/downloadForm/429146/4582853/429210/4582917/171/ca/tif/0.4096'
        b,final,status=get(url,512*1024);p=Form();p.feed(b.decode('utf-8',errors='replace'))
        out['form']={'http':status,'url':final,'sha256':hashlib.sha256(b).hexdigest(),'forms':p.form,'fields':p.fields,'email_field':any('mail' in str(f).lower() for f in p.fields),'captcha_present':bool(re.search(r'captcha|turnstile',b.decode('utf-8',errors='replace'),re.I))}
except Exception as e:out['error']=str(e)
out['submitted']=False
out['native_cells_recovered']=0
print('DM11_FORM_BEGIN');print(json.dumps(out,ensure_ascii=False,indent=2));print('DM11_FORM_END')
