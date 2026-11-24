"""DM20d: targeted exception scope and conservative graph crosswalk, no merge."""
import independent_dm20c as c
from pathlib import Path
from decimal import Decimal
import collections as C, datetime as dt, json,unicodedata,re
OUT=Path('custody/dm20d');OUT.mkdir(parents=True,exist_ok=True)
sources=[('ATM','custody/dm19b/ATM_NAP_mdb2832_20261117.zip','af45496d45406ca8d3d123232b9f42f1c8dcf5757aef5c123c56ce8e556396d3'),('FGC','custody/dm18/FGC_GTFS.zip','b0e681ec58695e546a7adece0140d53fb75f2fa3277a6d3f878f33d06e9b3378'),('TRAMBAIX','custody/dm19/TRAM_TBX.zip','953f3f775ce39643c6b1c26361e2c396b03ca610d13f5459139179a91aaa294e'),('TRAMBESÒS','custody/dm19/TRAM_TBS.zip','856e3b9ac8eb31bf3fa76299c12a515f8ae40469b4208491bf4ecfd25b80c405')]
ops={x[0] for x in sources[1:]}
def norm(s):
    return ' '.join(unicodedata.normalize('NFKC',s).casefold().split())
def stopkey(x):
    try:
        return norm(x['stop_name']),Decimal(x['stop_lat']),Decimal(x['stop_lon'])
    except Exception:return None
def audit(p,h):
    with c.acquire(p,h) as z:
        ag={x.get('agency_id',''):x['agency_name'] for x in c.rows(z,'agency')}
        route={x['route_id']:(ag[x.get('agency_id','')],x.get('route_short_name','')) for x in c.rows(z,'routes')}
        trips={x['trip_id']:x for x in c.rows(z,'trips')}
        stops={x['stop_id']:x for x in c.rows(z,'stops')}
        wd=c.DAY.strftime('%A').lower()
        active={x['service_id'] for x in c.rows(z,'calendar') if c.date(x['start_date'])<=c.DAY<=c.date(x['end_date']) and x[wd]=='1'}
        for x in c.rows(z,'calendar_dates'):
            if c.date(x['date'])==c.DAY:
                if x['exception_type']=='1':active.add(x['service_id'])
                else:active.discard(x['service_id'])
        active_ids={t for t,x in trips.items() if x['service_id'] in active}
        op={t:route[x['route_id']][0] for t,x in trips.items()}
        n=C.Counter();miss=C.Counter();miss_active=C.Counter();records=C.defaultdict(list);active_records=C.defaultdict(list);by_op=C.defaultdict(set)
        for x in c.rows(z,'stop_times'):
            t=x['trip_id'];n[t]+=1
            a,b=x.get('arrival_time',''),x.get('departure_time','')
            if not a or not b:
                timepoint=x.get('timepoint','') or 'ABSENT'
                window=bool(x.get('start_pickup_drop_off_window') or x.get('end_pickup_drop_off_window'))
                k=(timepoint,'WINDOW' if window else 'NO_WINDOW','BOTH_ABSENT' if not a and not b else 'ONE_ABSENT')
                miss[k]+=1
                if t in active_ids:miss_active[k]+=1
            if t in active_ids and op[t] in ops:
                active_records[t].append((int(x['stop_sequence']),x['stop_id'],a,b))
                by_op[op[t]].add(x['stop_id'])
        short=[{'trip_id':t,'operator':op[t],'stop_times':n[t],'active_on_date':t in active_ids} for t in trips if n[t]<2]
        def counts(k):return [{'timepoint':a,'window':b,'presence':d,'rows':v} for (a,b,d),v in sorted(k.items())]
        return {'active_ids':active_ids,'active_records':active_records,'stop_ids_by_op':by_op,'stops':stops,'route':route,'trips':trips,'op':op,'exception_report':{'trips_without_rows':sum(x['stop_times']==0 for x in short),'trips_one_row':sum(x['stop_times']==1 for x in short),'active_short_trips':sum(x['active_on_date'] for x in short),'short_trip_details':short,'missing_time_classes':counts(miss),'active_missing_time_classes':counts(miss_active)}}
def match(atm,direct,op):
    aa=atm['stop_ids_by_op'][op];dd=direct['stop_ids_by_op'][op]
    ia=C.defaultdict(list);idd=C.defaultdict(list)
    for s in aa:
        key=stopkey(atm['stops'][s])
        if key is not None:ia[key].append(s)
    for s in dd:
        key=stopkey(direct['stops'][s])
        if key is not None:idd[key].append(s)
    mapped={v[0]:ia[k][0] for k,v in idd.items() if len(v)==1 and len(ia.get(k,[]))==1}
    # Unique equality of published name and numeric coordinates; not authority for physical identity.
    crosswalk=[{'operator':op,'direct_stop_id':s,'atm_stop_id':a,'stop_name':direct['stops'][s]['stop_name'],'lat':direct['stops'][s]['stop_lat'],'lon':direct['stops'][s]['stop_lon']} for s,a in sorted(mapped.items())]
    def fingerprints(d,mapping=None):
        bag=C.Counter();unmapped=0;edges=C.Counter()
        for t,rs in d['active_records'].items():
            if d['op'][t]!=op:continue
            rr=sorted(rs);ids=[r[1] for r in rr]
            if mapping is not None:
                if not all(s in mapping for s in ids):unmapped+=1;continue
                ids=[mapping[s] for s in ids]
            label=d['route'][d['trips'][t]['route_id']][1]
            fp=(label,tuple((s,rr[i][2],rr[i][3]) for i,s in enumerate(ids)))
            bag[fp]+=1
            for a,b in zip(ids,ids[1:]):edges[label,a,b]+=1
        return bag,unmapped,edges
    a,_,ae=fingerprints(atm);d,u,de=fingerprints(direct,mapped)
    return {'atm_active_stops':len(aa),'direct_active_stops':len(dd),'unique_name_coordinate_matches':len(mapped),'direct_unmatched_or_ambiguous':len(dd)-len(mapped),'fully_mapped_direct_trips':sum(d.values()),'unmapped_direct_trips':u,'exact_trip_schedule_multiset_matches':sum((a&d).values()),'exact_distinct_directed_route_edges_matches':len(set(ae)&set(de)),'crosswalk':crosswalk,'merged_in_production':0,'note':'Strict exact name+numeric coordinate candidate crosswalk; no proximity assumptions or label-only merge. Trip fingerprints compare full ordered stop IDs and source time strings.'}
def main():
    try:
        all={n:audit(p,h) for n,p,h in sources}
        r={'status':'CHECKS_RECORDED','ATM_exception_report':all['ATM']['exception_report'],'reconciliation':{op:match(all['ATM'],all[op],op) for op in sorted(ops)},'production':'NO_GO'}
    except Exception as e:r={'status':'FAILED','error':str(e),'production':'NO_GO'}
    (OUT/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'status':r['status'],'error':r.get('error')},ensure_ascii=False))
if __name__=='__main__':main()
