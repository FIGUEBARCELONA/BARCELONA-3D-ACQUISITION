"""DM19b: alternative licensed ATM mirror and exact municipal population CSV."""
import independent_dm18b as common
import csv, gzip, hashlib, io, json, zipfile, collections, re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
OUT=Path('custody/dm19b');OUT.mkdir(parents=True,exist_ok=True);common.OUT=OUT
def req(x,m):
    if not x:raise ValueError(m)
def atm():
    url='https://files.mobilitydatabase.org/mdb-2832/mdb-2832-202611170155/mdb-2832-202611170155.zip'
    raw,m=common.get(url,cap=80*1024**2)
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        req(sum(i.file_size for i in z.infolist())<700*1024**2,'expanded cap')
        req(z.testzip() is None,'CRC')
        def rows(n):
            with z.open(n+'.txt') as f:
                yield from csv.DictReader(io.TextIOWrapper(f,encoding='utf-8-sig'))
        names=z.namelist();counts={}
        agency=list(rows('agency'));counts['agency']=len(agency)
        agencies={x.get('agency_id',''):x['agency_name'] for x in agency}
        routes={};stops=set();trips={};svc=set();shapes=set();dup=collections.Counter();fk=collections.Counter();groups=collections.Counter()
        for t in ('calendar','calendar_dates'):
            if t+'.txt' in names:
                counts[t]=0
                for x in rows(t):svc.add(x['service_id']);counts[t]+=1
        for x in rows('shapes'):
            counts['shapes']=counts.get('shapes',0)+1;shapes.add(x['shape_id'])
        for x in rows('routes'):
            counts['routes']=counts.get('routes',0)+1;k=x['route_id'];dup['route_id']+=k in routes;routes[k]=x.get('agency_id','');fk['route_agency']+=routes[k] not in agencies
        for x in rows('stops'):
            counts['stops']=counts.get('stops',0)+1;k=x['stop_id'];dup['stop_id']+=k in stops;stops.add(k)
        for x in rows('trips'):
            counts['trips']=counts.get('trips',0)+1;k=x['trip_id'];dup['trip_id']+=k in trips;trips[k]=x['route_id'];fk['trip_route']+=x['route_id'] not in routes;fk['trip_service']+=x['service_id'] not in svc;fk['trip_shape']+=bool(x.get('shape_id')) and x['shape_id'] not in shapes
            groups[agencies.get(routes.get(x['route_id']), 'UNKNOWN')]+=1
        # Stream all stop times rather than holding an expanded national feed in memory.
        rc=collections.Counter();sc=collections.defaultdict(set)
        for x in rows('stop_times'):
            counts['stop_times']=counts.get('stop_times',0)+1;trip=x['trip_id'];stop=x['stop_id'];fk['stop_time_trip']+=trip not in trips;fk['stop_time_stop']+=stop not in stops
            ag=agencies.get(routes.get(trips.get(trip)), 'UNKNOWN')
            if ag.upper() in ('TMB','TRAMBAIX','TRAMBESÒS','FGC'):rc[ag]+=1;sc[ag].add(stop)
        selected={}
        for name in sorted(set(agencies.values())):
            if name.upper() in ('TMB','TRAMBAIX','TRAMBESÒS','FGC'):
                selected[name]={'routes':sum(agencies.get(a)==name for a in routes.values()),'trips':groups[name],'stop_times':rc[name],'distinct_stop_ids_referenced':len(sc[name])}
        feed=list(rows('feed_info')) if 'feed_info.txt' in names else []
        src=common.save('ATM_NAP_mdb2832_20261117.zip',raw,m,False)
        return {'source':src,'sha1':hashlib.sha1(raw).hexdigest(),'members':names,'counts':counts,'duplicate_ids':dict(dup),'foreign_keys':dict(fk),'publisher':feed,'selected_operators':selected,'scope':'ATM simplified feed, all Catalonia; not sum with standalone operators','temporal_order_full_check':False,'actual_occupancy_verified':False}
def population():
    url='https://opendata-ajuntament.barcelona.cat/data/dataset/2f6e0561-30f4-44a0-8446-e27442d4754c/resource/eb82adf2-a7b0-40e6-9624-b4b9eff23018/download'
    raw,m=common.get(url,cap=2*1024**2)
    s=raw.decode('utf-8-sig');dialect=csv.Sniffer().sniff(s[:4000],delimiters=',;');r=csv.DictReader(io.StringIO(s),dialect=dialect);rs=list(r);fields=r.fieldnames
    src=common.save('2025_pad_mdbas.csv',raw,m)
    def field(*opts):return next((x for x in fields if x.lower() in opts),None)
    v=field('nombre','valor','poblacio','població');d=field('codi_districte');b=field('codi_barri');a=field('any');sec=field('seccio_censal','codi_seccio_censal');dn=field('nom_districte');bn=field('nom_barri')
    result={'source':src,'fields':fields,'rows':len(rs),'mapping':{'value':v,'district':d,'barri':b,'year':a,'section':sec},'sample':rs[:3]}
    req(all((v,d,b,a)),'missing field mapping')
    ds=collections.Counter();bs=collections.Counter();seen=set();bnames={};dnames={};excluded=[]
    for x in rs:
        req(x[a]=='2025','year mismatch')
        di,bi=int(x[d]),int(x[b]);n=int(x[v]);req(n>=0,'negative')
        if di not in range(1,11) or bi not in range(1,74):
            excluded.append(x);continue
        k=tuple(x[f] for f in fields if f!=v);req(k not in seen,'duplicate dimension');seen.add(k)
        ds[di]+=n;bs[(di,bi)]+=n
        if dn:dnames[di]=x[dn]
        if bn:bnames[bi]=x[bn]
    req(len(ds)==10 and len(bs)==73 and {bi for di,bi in bs}==set(range(1,74)),'territorial coverage')
    result.update(districts=[{'code':k,'name':dnames.get(k),'total':v} for k,v in sorted(ds.items())],barris=[{'district':di,'code':bi,'name':bnames.get(bi),'total':v} for (di,bi),v in sorted(bs.items())],sum_in_units=sum(ds.values()),excluded=excluded,unique_dimensions=len(seen),geographic_coverage='10 districts and 73 barris; reconciliation to published city total pending')
    return result
def main():
    def safe(q):
        k,f=q
        try:return k,{'status':'CHECKS_RECORDED','data':f()}
        except Exception as e:return k,{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=2) as p:r=dict(p.map(safe,[('ATM_mobilitydatabase',atm),('population_2025',population)]))
    r.update(canonical_EAM_closed=0,production='NO_GO')
    (OUT/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    (OUT/'README.md').write_text('DM-19b. ATM simplified GTFS archive from Mobility Database mdb-2832, dated 17 November 2026; Powered by MITRAMS https://www.transportes.gob.es/ ; licence https://nap.transportes.gob.es/licencia-datos . Official municipal Padró 2025 (Ajuntament de Barcelona, Oficina Municipal de Dades, CC BY 4.0), resource eb82adf2-a7b0-40e6-9624-b4b9eff23018. Original archive retained without alteration if successful. No claim of latest or real traffic. NO-GO.\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error'),'counts':v.get('data',{}).get('counts')} for k,v in r.items() if isinstance(v,dict)},ensure_ascii=False))
if __name__=='__main__':main()
