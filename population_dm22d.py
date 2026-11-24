"""DM22d: exact 2025/2026 population extraction and territorial reconciliation."""
import gzip,json,hashlib,collections,unicodedata,csv,io,math
from pathlib import Path
O=Path('custody/dm22d');O.mkdir(parents=True,exist_ok=True)
p=Path('custody/dm22b/population_candidate_0.json.gz');b=p.read_bytes();raw=gzip.decompress(b)
assert hashlib.sha256(b).hexdigest()=='92e8ffddef5443d9c7726a8ba615872ebcdc2ddcda517aec58fb29831de19917'
assert hashlib.sha256(raw).hexdigest()=='2a2959d1cbe2ac7616466d9efa11c0e23ea7ba091039739fd922ed218619c048'
j=json.loads(raw)
def norm(s):return ''.join(c for c in unicodedata.normalize('NFKD',s).casefold() if c.isalnum())
def req(x,m):
    if not x:raise ValueError(m)
def main():
    md=j['data'][0];req(md['id']=='yzlntdm2fs' and md['license']=='CC BY 4.0','metadata')
    time=j['dims'][0]['values'][0]['values'];city=j['dims'][1]['values'][0];districts=city['values']
    req(len(time)==146 and len(districts)==10 and sum(len(d['values']) for d in districts)==73,'dimension counts')
    nodes=[city]+[d for d in districts]+[b for d in districts for b in d['values']]
    ids={x['id'] for x in nodes};dates={t['id'] for t in time}
    req(len(ids)==84 and len(dates)==146 and len(j['values'])==12264,'matrix dimensions')
    matrix={}
    for v in j['values']:
        k=v['dim0'],v['dim1'];req(k not in matrix and k[0] in dates and k[1] in ids,'invalid matrix key')
        matrix[k]=v
    req(len(matrix)==84*146,'matrix coverage')
    prior=json.loads(Path('custody/dm20b/primary-population-reconciliation.json').read_text())['rows']
    prior_map={(norm(x['district']),norm(x['barri'])):x for x in prior}
    official25={'eixample':278209,'santmarti':253139,'santsmontjuic':196300,'hortaguinardo':183009,'noubarris':182755,'santandreu':157674,'sarriasantgervasi':153673,'gracia':127603,'ciutatvella':115311,'lescorts':84393}
    def get(y,x):
        k='t_dia_'+str(y)+'-01-01T00:00:00Z',x['id'];v=matrix[k]
        n=v['value'];req(v['secret'] is False and isinstance(n,(int,float)) and not isinstance(n,bool) and math.isfinite(n) and n==int(n) and n>=0,'invalid selected value')
        return int(n)
    result=[];dt=[]
    for d in districts:
        name=d['label'];key=norm(name)
        r={'district':name,'source_id':d['id'],'2025':get(2025,d),'2026':get(2026,d),'barris':len(d['values'])}
        for y in (2025,2026):req(sum(get(y,b) for b in d['values'])==r[str(y)],'district subtotal '+name+str(y))
        req(r['2025']==official25[key],'DM19 district mismatch '+name)
        dt.append(r)
        for b in d['values']:
            p=prior_map[(key,norm(b['label']))];v25=get(2025,b);v26=get(2026,b)
            req(v26==p['population'],'DM20 2026 mismatch')
            result.append({'code':p['code'],'district':name,'barri':b['label'],'source_id':b['id'],'population_2025':v25,'population_2026':v26,'change':v26-v25})
    req({r['code'] for r in result}==set(range(1,74)),'codes')
    for y,n in [(2025,1732066),(2026,1729963)]:
        req(get(y,city)==n and sum(r[str(y)] for r in dt)==n and sum(r['population_'+str(y)] for r in result)==n,'city total')
    result.sort(key=lambda r:r['code'])
    buf=io.StringIO();w=csv.DictWriter(buf,fieldnames=list(result[0]));w.writeheader();w.writerows(result)
    csvb=buf.getvalue().encode();(O/'population_2025_2026.csv').write_bytes(csvb)
    return {'status':'CHECKS_RECORDED','source_url':'https://portaldades.ajuntament.barcelona.cat/ca/estad%C3%ADstiques/yzlntdm2fs','source_hash':hashlib.sha256(raw).hexdigest(),'source_metadata':{'title':md['title'],'license':md['license'],'publishDate':md['publishDate'],'docSource':md['docSource']},'matrix_cells':len(matrix),'territorial_units':len(ids),'time_points':len(dates),'selected_cells':168,'2025_city':get(2025,city),'2026_city':get(2026,city),'district_subtotals_checked':20,'city_total_checks':6,'DM19_district_exact_matches':10,'DM20_barri_2026_exact_matches':73,'2025_barris_primary':73,'districts':dt,'rows':result,'csv_sha256':hashlib.sha256(csvb).hexdigest(),'production':'NO_GO','canonical_EAM_closed':0}
try:r=main()
except Exception as e:r={'status':'FAILED','error':str(e),'production':'NO_GO'}
(O/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
print(json.dumps({k:v for k,v in r.items() if k!='rows'},ensure_ascii=False))
