"""DM20b: inspect official published HTML for embedded primary aggregate data."""
import urllib.request, urllib.parse, re,json,hashlib
from html.parser import HTMLParser
from pathlib import Path
OUT=Path('custody/dm20b');OUT.mkdir(parents=True,exist_ok=True)
U='https://dades.ajuntament.barcelona.cat/informe-padro-estrangers/ca/seccio_3.html'
class P(HTMLParser):
    def __init__(self):super().__init__(convert_charrefs=True);self.scripts=[];self.iframes=[];self.links=[];self.cur=None
    def handle_starttag(self,t,a):
        a=dict(a)
        if t=='script':self.cur={'attrs':a,'text':[]}
        elif t=='iframe':self.iframes.append({k:v[:400] for k,v in a.items()})
        elif t=='a' and 'href'in a and len(a['href'])<1000:self.links.append(a['href'])
    def handle_data(self,s):
        if self.cur is not None:self.cur['text'].append(s)
    def handle_endtag(self,t):
        if t=='script' and self.cur is not None:
            self.cur['text']=''.join(self.cur['text']);self.scripts.append(self.cur);self.cur=None
def main():
    r={'url':U,'status':'FAILED'}
    try:
        with urllib.request.urlopen(urllib.request.Request(U,headers={'User-Agent':'Barcelona3D public data QA','Accept-Encoding':'identity'}),timeout=45) as q:
            b=q.read(48*1024**2+1)
            if len(b)>48*1024**2:raise ValueError('cap')
            r.update(http=q.status,bytes=len(b),sha256=hashlib.sha256(b).hexdigest(),content_type=q.headers.get('Content-Type'))
        s=b.decode('utf-8');p=P();p.feed(s)
        r['iframed_sources']=p.iframes[:30]
        r['links']=[x for x in p.links if re.search(r'csv|xlsx|pdf|taula|table|dades',x,re.I)][:50]
        r['scripts']=[{'attrs':{k:v[:300] for k,v in x['attrs'].items()},'size':len(x['text']),'contains_neighborhood':bool(re.search(r'Sant Antoni|Sant.Antoni|Raval',x['text'],re.I)),'contains_target_value':bool(re.search(r'38978|38[.,]978',x['text']))} for x in p.scripts[:100]]
        contexts={}
        for term in ['Sant Antoni','Sant.Antoni','38.978','38978','40.136','40136','50.863','50863','1.729.963','1729963','<table','reactable','DT::','data-for']:
            rr=list(re.finditer(re.escape(term),s,re.I))
            contexts[term]={'count':len(rr),'contexts':[s[max(0,m.start()-100):m.end()+150] for m in rr[:3]]}
        r['contexts']=contexts
        # Preserve embedded source JSON only if a specific script contains both known row labels and exact numbers.
        found=[]
        for i,x in enumerate(p.scripts):
            t=x['text']
            if x['attrs'].get('type')=='application/json' and re.search(r'Sant Antoni|Sant.Antoni',t) and re.search(r'38978|38[.,]978',t):
                raw=t.encode();name=f'embedded-table-candidate-{i}.json';json.loads(t);(OUT/name).write_bytes(raw);found.append({'path':str(OUT/name),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
        r['embedded_candidates']=found;r['status']='INSPECTED_NOT_VALIDATED'
    except Exception as e:r['error']=str(e)
    (OUT/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in r.items() if k not in ('contexts','scripts')},ensure_ascii=False))
if __name__=='__main__':main()
