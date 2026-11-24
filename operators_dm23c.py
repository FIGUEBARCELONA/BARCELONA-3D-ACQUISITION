"""DM23c: source-level target checks of AMB and Generalitat direct feeds."""
import sources_dm23 as c
import io,zipfile,json,csv,collections,hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
O=Path('custody/dm23c');O.mkdir(parents=True,exist_ok=True)
def audit(name,u,prefix,operators,cap):
    raw,m=c.get(u,cap)
    old=json.loads(Path('custody/dm22b/report.json').read_text())['ATM_full']['data']['target_rows']
    targets={x['trip_id']:x for x in old if x['simplified_active'] and x['simplified_operator'] in operators}
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        c.req(sum(x.file_size for x in z.infolist())<=750*1024**2,'expanded cap');c.req(z.testzip() is None,'CRC')
        def rows(n):
            if n+'.txt' not in z.namelist():return
            with z.open(n+'.txt') as f:
                for x in csv.DictReader(io.TextIOWrapper(f,encoding='utf-8-sig')):
                    yield {k.strip():v for k,v in x.items() if k is not None}
        trips={x['trip_id']:x for x in rows('trips')}
        matches={t:[k for k in [t,t.removeprefix(prefix)] if k in trips] for t in sorted(targets)}
        ids={k for a in matches.values() for k in a}
        active=set();cal_status='CHECKS_RECORDED'
        try:
            gt=c.gt;wd=gt.DAY.strftime('%A').lower()
            for x in rows('calendar'):
                if gt.date(x['start_date'])<=gt.DAY<=gt.date(x['end_date']) and x[wd]=='1':active.add(x['service_id'])
            for x in rows('calendar_dates'):
                if gt.date(x['date'])==gt.DAY:
                    if x['exception_type']=='1':active.add(x['service_id'])
                    elif x['exception_type']=='2':active.discard(x['service_id'])
                    else:raise ValueError('exception')
        except Exception as e:cal_status='FAILED: '+str(e)
        routes={x['route_id']:x for x in rows('routes')};agency={x.get('agency_id',''):x['agency_name'] for x in rows('agency')}
        groups=collections.defaultdict(list)
        for x in rows('stop_times'):
            if x['trip_id'] in ids:groups[x['trip_id']].append(x)
        out=[]
        for t,a in matches.items():
            out.append({'target':t,'target_operator':targets[t]['simplified_operator'],'candidates':[{'trip_id':k,'route':trips[k]['route_id'],'agency':agency.get(routes[trips[k]['route_id']].get('agency_id','')),'active':trips[k]['service_id'] in active if cal_status=='CHECKS_RECORDED' else None,'stop_rows':len(groups[k]),'timed_rows':sum(bool(x.get('arrival_time')) and bool(x.get('departure_time')) for x in groups[k])} for k in a]})
        evidence=json.dumps({'trips':{k:trips[k] for k in ids},'stop_times':dict(groups)},ensure_ascii=False).encode()
        p=O/(name+'_target_extract.json');p.write_bytes(evidence)
        return {'source':m,'CRC_verified':True,'original_archive_not_persisted':True,'target_extract_sha256':hashlib.sha256(evidence).hexdigest(),'agencies':sorted(set(agency.values())),'calendar_status':cal_status,'trip_count':len(trips),'target_count':len(targets),'candidate_count':sum(bool(a) for a in matches.values()),'results':out,'recovered_exact_service_identity':0,'no_imputation':True}
def main():
    tasks=[('AMB','https://www.ambmobilitat.cat/OpenData/google_transit.zip','AMB_',{'DIREXIS TUSGSAL'},32*1024**2),('GEN','https://territori.gencat.cat/web/.content/home/serveis/visors-cartografia/bases-cartografiques/infraestructures-mobilitat/autobusos-interurbans/busos-interurbans-GTFS.zip','GEN_',{'Barcelona Bus, SL'},80*1024**2)]
    def safe(x):
        try:return x[0],{'status':'CHECKS_RECORDED','data':audit(*x)}
        except Exception as e:return x[0],{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=2) as e:r=dict(e.map(safe,tasks))
    r.update(production='NO_GO',canonical_EAM_closed=0)
    (O/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error')} for k,v in r.items() if isinstance(v,dict)}))
if __name__=='__main__':main()
