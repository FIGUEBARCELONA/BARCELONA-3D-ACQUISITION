"""DM23 bounded acquisition of public monthly statistics, primary operator and metadata."""
import urllib.request,urllib.parse,hashlib,zipfile,csv,io,json,collections,gzip,re,datetime as dt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import independent_dm20c as gt
O=Path('custody/dm23');O.mkdir(parents=True,exist_ok=True)
def req(x,m):
    if not x:raise ValueError(m)
def sha(b):return hashlib.sha256(b).hexdigest()
def get(u,cap):
    with urllib.request.urlopen(urllib.request.Request(u,headers={'Accept-Encoding':'identity','User-Agent':'Barcelona3D public data QA'}),timeout=35) as r:
        b=r.read(cap+1);req(len(b)<=cap and r.status==200,'cap or HTTP')
        return b,{'url':r.geturl(),'bytes':len(b),'sha256':sha(b),'content_type':r.headers.get('Content-Type'),'last_modified':r.headers.get('Last-Modified')}
def save(n,b):
    p=O/n;p.write_bytes(b);return {'path':str(p),'sha256':sha(b),'bytes':len(b)}
def port(name,u):
    b,m=get(u,16*1024**2)
    s=b.decode('utf-8-sig');d=csv.Sniffer().sniff(s[:8000],delimiters=',;\t');r=csv.DictReader(io.StringIO(s),dialect=d);rows=list(r)
    req(rows and all(None not in x for x in rows),'CSV shape')
    src=save(name+'.csv.gz',gzip.compress(b,mtime=0))
    return {'source':m,'custody':src,'rows':len(rows),'columns':r.fieldnames,'samples':rows[:4],'last':rows[-2:],'aggregation_verified':False}
def renfe():
    u='https://ssl.renfe.com/ftransit/Fichero_CER_FOMENTO/fomento_transit.zip'
    b,m=get(u,32*1024**2)
    old=json.loads(Path('custody/dm22b/report.json').read_text())['ATM_full']['data']['target_rows']
    targets={x['trip_id']:x for x in old if x['simplified_active'] and x['simplified_operator']=='RENFE OPERADORA'}
    with zipfile.ZipFile(io.BytesIO(b)) as z:
        req(sum(x.file_size for x in z.infolist())<350*1024**2,'expanded cap');req(z.testzip() is None,'CRC')
        trips={x['trip_id']:x for x in gt.rows(z,'trips')}
        # Prefix-only candidate; no route/time equivalence assumed.
        match={}
        for t in targets:
            a=[k for k in [t,t.removeprefix('REG_B_')] if k in trips]
            if a:match[t]=a
        active=set()
        for x in gt.rows(z,'calendar'):
            if gt.date(x['start_date'])<=gt.DAY<=gt.date(x['end_date']) and x[gt.DAY.strftime('%A').lower()]=='1':active.add(x['service_id'])
        for x in gt.rows(z,'calendar_dates'):
            if gt.date(x['date'])==gt.DAY:
                if x['exception_type']=='1':active.add(x['service_id'])
                else:active.discard(x['service_id'])
        selected={v for a in match.values() for v in a};groups=collections.defaultdict(list)
        for x in gt.rows(z,'stop_times'):
            if x['trip_id'] in selected:groups[x['trip_id']].append(x)
        result=[{'target':t,'candidates':[{'id':v,'active':trips[v]['service_id'] in active,'stop_rows':len(groups[v]),'complete_time_rows':sum(bool(x.get('arrival_time')) and bool(x.get('departure_time')) for x in groups[v])} for v in match.get(t,[])]} for t in targets]
        return {'source':m,'archive_crc':True,'custody':save('renfe_cercanias.zip',b),'trip_count':len(trips),'target_count':len(targets),'candidate_count':len(match),'results':result,'active_trip_candidates_with_two_stops':sum(any(v['active'] and v['stop_rows']>=2 for v in r['candidates']) for r in result),'recovered_exact_service_identity':0,'sample_trip_ids':list(trips)[:10]}
def metadata():
    u='https://maps.icgc.cat/vector01/rest/services/divisions_administratives_wms/MapServer/12?f=json'
    b,m=get(u,1024**2);j=json.loads(b);req(j.get('name'),'layer name')
    return {'source':m,'custody':save('icgc_layer12.json',b),'name':j.get('name'),'description':j.get('description'),'copyrightText':j.get('copyrightText'),'minScale':j.get('minScale'),'maxScale':j.get('maxScale'),'geometryType':j.get('geometryType'),'editingInfo':j.get('editingInfo'),'fields':[x.get('name') for x in j.get('fields',[])]}
def main():
    tasks=[('passengers',lambda:port('port_passatgers_2025','https://opendata.portdebarcelona.cat/es/dataset/4165236c-3275-402e-9d98-593e36fb0ef6/resource/da7e18b6-e25b-447d-885c-04475e90b6e3/download/trafic_passatgers_2025.csv')),('ships',lambda:port('port_vaixells_2025','https://opendata.portdebarcelona.cat/en/dataset/5b8c4bdd-94c0-4273-83b0-bac1e2ee4334/resource/abbc81ff-802b-449b-9e5f-cce7a1fe57d3/download/trafic_vaixells_2025.csv')),('cargo',lambda:port('port_mercaderies_2025','https://opendata.portdebarcelona.cat/ca/dataset/0b02af8e-8d2c-4c61-affa-e151f20ab45e/resource/dd4b292d-a91a-4512-8b48-0c826eef3aaf/download/trafic_mercaderies_envas_2025.csv')),('renfe',renfe),('icgc_layer',metadata)]
    def safe(x):
        k,f=x
        try:return k,{'status':'CHECKS_RECORDED','data':f()}
        except Exception as e:return k,{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=4) as e:r=dict(e.map(safe,tasks))
    r.update(production='NO_GO',canonical_EAM_closed=0)
    (O/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    (O/'README.md').write_text('DM-23. Port de Barcelona — Autoritat Portuària de Barcelona, open data statistics; attribution and original source URLs in report. Renfe Data CC BY 4.0. ICGC CC BY 4.0. No imputation, no source substitution; NO-GO.\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error')} for k,v in r.items() if isinstance(v,dict)}))
if __name__=='__main__':main()
