"""DM22c: parse original public OMD statistical response without further network."""
import gzip,json,hashlib,collections,re,unicodedata,csv,io
from pathlib import Path
O=Path('custody/dm22c');O.mkdir(parents=True,exist_ok=True)
p=Path('custody/dm22b/population_candidate_0.json.gz');b=p.read_bytes();raw=gzip.decompress(b)
assert hashlib.sha256(b).hexdigest()=='92e8ffddef5443d9c7726a8ba615872ebcdc2ddcda517aec58fb29831de19917'
assert hashlib.sha256(raw).hexdigest()=='2a2959d1cbe2ac7616466d9efa11c0e23ea7ba091039739fd922ed218619c048'
j=json.loads(raw)
def profile(v,depth=0):
    if isinstance(v,dict):
        return {k:profile(x,depth+1) for k,x in v.items()} if depth<5 else {'keys':list(v)[:30]}
    if isinstance(v,list):return {'length':len(v),'first':profile(v[0],depth+1) if v and depth<6 else None,'last':profile(v[-1],depth+1) if v and depth<4 else None}
    return str(v)[:300]
def walk(v,p=''):
    if isinstance(v,dict):
        for k,x in v.items():yield from walk(x,p+'/'+k)
    elif isinstance(v,list):
        yield (p,v)
        if len(v)<20:
            for i,x in enumerate(v):yield from walk(x,p+'/'+str(i))
r={'schema':profile(j),'arrays':[{'path':p,'length':len(x),'first':str(x[0])[:500] if x else None} for p,x in walk(j) if len(x)>=73]}
(O/'schema.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(r,ensure_ascii=False)[:14000])
