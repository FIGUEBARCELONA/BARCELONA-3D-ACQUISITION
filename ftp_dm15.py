"""DM-15: bounded, read-only public FTP and HTTPS directory discovery."""
import concurrent.futures, ftplib, json, socket, urllib.request, urllib.parse, re, hashlib
from html.parser import HTMLParser
from pathlib import Path
class Links(HTMLParser):
    def __init__(self):super().__init__();self.links=[]
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag=='a' and 'href' in a:self.links.append(a['href'])
def web(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'Barcelona3D open data QA'}),timeout=12) as r:
            b=r.read(262145)
            if len(b)>262144:raise ValueError('directory response over cap')
            p=Links();p.feed(b.decode('utf-8','replace'))
            return {'url':r.geturl(),'http':r.status,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest(),'links':p.links[:250],'title':re.findall(r'<title>(.*?)</title>',b.decode('utf-8','replace'),re.I|re.S)[:1]}
    except Exception as e:return {'url':url,'error':str(e)}
def ftp():
    host='ftp.icgc.cat';out={'host':host,'login':'anonymous only','retrieved_bytes':0}
    try:
        out['dns']=sorted({a[4][0] for a in socket.getaddrinfo(host,21,type=socket.SOCK_STREAM)})
        with ftplib.FTP(host,timeout=12) as f:
            f.login();out['anonymous_login']=True
            out['directory_entries']=[]
            # Only inspect public root listing; no recursive enumeration.
            def add(line):
                if len(out['directory_entries'])>=250:raise ValueError('listing cap')
                out['directory_entries'].append(line)
            f.retrlines('LIST',add)
    except Exception as e:out['error']=str(e)
    return out
urls=['https://ftp.icgc.cat/descarregues/','https://ftp.icgc.cat/descarregues/CatLCNet/','https://datacloud.ide.cat/']
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    f=pool.submit(ftp);w=[pool.submit(web,u) for u in urls]
    report={'ftp':f.result(),'https':[q.result() for q in w],'native_cells_recovered':0,'email_submitted':False,'canonical_EAM_closed':0,'production':'NO_GO'}
p=Path('custody/dm15');p.mkdir(parents=True,exist_ok=True)
(p/'ftp-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print('DM15_RESULT '+json.dumps(report,ensure_ascii=False))
