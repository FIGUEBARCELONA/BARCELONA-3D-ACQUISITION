"""DM20: offline exact GTFS archive readback, calendar and graph QA; Y9 source inventory."""
import csv,io,zipfile,hashlib,json,gzip,collections,datetime as dt,math,urllib.request,urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
OUT=Path('custody/dm20');OUT.mkdir(parents=True,exist_ok=True)
BASE='https://analisi.transparenciacatalunya.cat'
DAY=dt.date(2026,11,24)
def req(x,m):
    if not x:raise ValueError(m)
def sha(b):return hashlib.sha256(b).hexdigest()
def date(x):return dt.datetime.strptime(x,'%Y%m%d').date()
def sec(x):
    a,b,c=map(int,x.split(':'));req(a>=0 and 0<=b<60 and 0<=c<60,'invalid time');return 3600*a+60*b+c
def rows(z,n):
    if n+'.txt' in z.namelist():
        with z.open(n+'.txt') as f:yield from csv.DictReader(io.TextIOWrapper(f,encoding='utf-8-sig'))
def acquire(path,expected):
    raw=Path(path).read_bytes();req(sha(raw)==expected,'source hash mismatch '+path)
    z=zipfile.ZipFile(io.BytesIO(raw));req(z.testzip() is None,'CRC');return z
def analyze(path,expected,feed):
    with acquire(path,expected) as z:
        agency={x.get('agency_id',''):x['agency_name'] for x in rows(z,'agency')}
        routes={x['route_id']:x for x in rows(z,'routes')}
        stops={x['stop_id']:x for x in rows(z,'stops')}
        trips={x['trip_id']:x for x in rows(z,'trips')}
        def op(t):
            r=routes[trips[t]['route_id']];return agency[r.get('agency_id','')]
        issues=collections.Counter();cal={};ranges=[];exc={};services=set()
        week=['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
        for x in rows(z,'calendar'):
            s=x['service_id'];a,b=date(x['start_date']),date(x['end_date'])
            req(a<=b and (b-a).days<=3660,'calendar range invalid')
            req(all(x[k] in ('0','1') for k in week),'calendar flag')
            if s in cal:issues['duplicate_calendar_service']+=1
            cal[s]=(a,b,[int(x[k]) for k in week]);ranges.extend([a,b]);services.add(s)
        for x in rows(z,'calendar_dates'):
            s=x['service_id'];d=date(x['date']);t=x['exception_type']
            req(t in ('1','2'),'bad exception')
            if (s,d) in exc:issues['duplicate_exception']+=1
            exc[s,d]=t;ranges.append(d);services.add(s)
        req(ranges,'missing calendar')
        def active(s,d):
            if (s,d) in exc:return exc[s,d]=='1'
            if s not in cal:return False
            a,b,w=cal[s];return a<=d<=b and bool(w[d.weekday()])
        active_ids={t for t,x in trips.items() if active(x['service_id'],DAY)}
        active_s={s for s in services if active(s,DAY)}
        opcounts=collections.Counter(op(t) for t in active_ids)
        # Full effective calendar: sum service-day template counts; no frequency expansion.
        svc_count=collections.Counter(x['service_id'] for x in trips.values());lo,hi=min(ranges),max(ranges)
        req((hi-lo).days<=3660,'calendar horizon cap')
        effective=collections.Counter()
        for s,n in svc_count.items():
            if s in cal:
                a,b,w=cal[s]
                for i in range((b-a).days+1):
                    d=a+dt.timedelta(days=i)
                    if w[d.weekday()]:effective[d]+=n
        for (s,d),v in exc.items():
            n=svc_count[s];base=s in cal and cal[s][0]<=d<=cal[s][1] and bool(cal[s][2][d.weekday()])
            effective[d]+=n*((v=='1')-base)
        req(effective[DAY]==len(active_ids),'calendar independent count mismatch')
        positive=sorted(d for d,n in effective.items() if n>0)
        freq=collections.defaultdict(list);freqrows=0
        for x in rows(z,'frequencies'):
            freqrows+=1;t=x['trip_id']
            try:
                a,b,h=sec(x['start_time']),sec(x['end_time']),int(x['headway_secs']);e=x.get('exact_times','') or '0'
                req(t in trips and a<b and h>0 and e in ('0','1'),'frequency fields')
                freq[t].append((a,b,h,e))
            except Exception:issues['invalid_frequency']+=1
        for rr in freq.values():
            rr.sort();issues['overlap_frequency']+=sum(x[1]>y[0] for x,y in zip(rr,rr[1:]))
        counts=collections.Counter();groups=collections.defaultdict(list)
        for x in rows(z,'stop_times'):
            counts['stop_times']+=1;t=x['trip_id'];st=x['stop_id']
            if t not in trips or st not in stops:issues['stop_time_fk']+=1;continue
            try:
                n=int(x['stop_sequence']);req(n>=0,'negative sequence')
                aa=x.get('arrival_time','');bb=x.get('departure_time','')
                a=sec(aa) if aa else None;b=sec(bb) if bb else None
                if a is None or b is None:counts['missing_time_rows']+=1
                if a is not None and b is not None and a>b:issues['departure_before_arrival']+=1
                if (a is not None and a>=86400) or (b is not None and b>=86400):counts['overnight_rows']+=1
                groups[t].append((n,a,b,st))
            except Exception:issues['invalid_stop_time']+=1
        edges=collections.Counter();nodes=set();op_edges=collections.Counter();op_nodes=collections.defaultdict(set);route_patterns=collections.defaultdict(set)
        for t,rs in groups.items():
            rs.sort(key=lambda x:x[0]);issues['duplicate_sequence']+=len(rs)-len({x[0] for x in rs})
            if len(rs)<2:issues['trip_fewer_than_two_stops']+=1
            if rs[0][1] is None or rs[0][2] is None or rs[-1][1] is None or rs[-1][2] is None:issues['missing_endpoint_time']+=1
            last=None
            for _,a,b,st in rs:
                for v in (a,b):
                    if v is not None:
                        if last is not None and v<last:issues['decreasing_time_values']+=1
                        last=v
            if t not in active_ids:continue
            r=trips[t]['route_id'];operator=op(t)
            route_patterns[(operator,r)].add(tuple(x[3] for x in rs))
            for x in rs:nodes.add(x[3]);op_nodes[operator].add(x[3])
            for x,y in zip(rs,rs[1:]):edges[operator,r,x[3],y[3]]+=1
        issues['trips_without_stop_times']=len(set(trips)-set(groups))
        for k in edges:op_edges[k[0]]+=1
        # One canonical snapshot only. Namespace route+stop IDs; no cross-feed physical merges.
        if feed=='ATM':
            dest=OUT/'atm_active_20261124_edges.csv.gz'
            raw=io.StringIO();w=csv.writer(raw,lineterminator='\n');w.writerow(['feed','operator','route_id','from_stop_id','to_stop_id','active_trip_template_edge_count'])
            for k,n in sorted(edges.items()):w.writerow([feed,*k,n])
            b=raw.getvalue().encode();dest.write_bytes(gzip.compress(b,mtime=0));req(gzip.decompress(dest.read_bytes())==b,'graph roundtrip')
            graph={'path':str(dest),'sha256':sha(dest.read_bytes()),'raw_sha256':sha(b),'rows':len(edges)}
        else:graph=None
        info={'feed':feed,'source':path,'source_sha256':expected,'trips':len(trips),'stop_times':counts['stop_times'],'calendar_service_ids':len(cal),'exceptions':len(exc),'calendar_declared_extremes':[str(lo),str(hi)],'effective_extremes':[str(positive[0]),str(positive[-1])] if positive else [],'effective_service_days':len(positive),'active_date':str(DAY),'active_services':len(active_s),'active_trip_templates':len(active_ids),'active_frequency_templates':len(active_ids&set(freq)),'frequency_rows':freqrows,'frequency_exact_rows':sum(e=='1' for rr in freq.values() for a,b,h,e in rr),'active_operator_templates':dict(opcounts),'counts':dict(counts),'issues':dict(issues),'active_nodes':len(nodes),'active_route_directed_edges':len(edges),'edge_template_occurrences':sum(edges.values()),'active_operator_edges':dict(op_edges),'active_operator_nodes':{k:len(v) for k,v in op_nodes.items()},'distinct_route_patterns':sum(map(len,route_patterns.values())),'graph':graph,'frequency_expanded':False}
        # Reconciliation candidates: route short name+operator; exact stop IDs compared separately.
        routekeys=collections.defaultdict(list)
        for r,x in routes.items():routekeys[(agency[x.get('agency_id','')],x.get('route_short_name',''))].append(r)
        return info,routekeys,stops
def transport():
    sources=[('ATM','custody/dm19b/ATM_NAP_mdb2832_20261117.zip','af45496d45406ca8d3d123232b9f42f1c8dcf5757aef5c123c56ce8e556396d3'),('FGC','custody/dm18/FGC_GTFS.zip','b0e681ec58695e546a7adece0140d53fb75f2fa3277a6d3f878f33d06e9b3378'),('TBX','custody/dm19/TRAM_TBX.zip','953f3f775ce39643c6b1c26361e2c396b03ca610d13f5459139179a91aaa294e'),('TBS','custody/dm19/TRAM_TBS.zip','856e3b9ac8eb31bf3fa76299c12a515f8ae40469b4208491bf4ecfd25b80c405')]
    reports={};indices={}
    for f,p,h in sources:
        info,rr,ss=analyze(p,h,f);reports[f]=info;indices[f]=(rr,ss)
    ar,ast=indices['ATM'];reconciliation={}
    for f in ['FGC','TBX','TBS']:
        rr,ss=indices[f];matches=[];unmatched=[]
        for key,ids in rr.items():
            c=ar.get(key,[])
            target=matches if c else unmatched;target.append({'operator':key[0],'route_short_name':key[1],'standalone_ids':ids,'ATM_candidate_ids':c,'identity_verified':False})
        common=set(ss)&set(ast)
        exact_coords=sum(ss[k].get('stop_lat')==ast[k].get('stop_lat') and ss[k].get('stop_lon')==ast[k].get('stop_lon') for k in common)
        reconciliation[f]={'route_label_candidates':matches,'unmatched_route_labels':unmatched,'shared_raw_stop_ids':len(common),'shared_raw_stop_ids_exact_coordinate_strings':exact_coords,'merged_stops':0,'added_standalone_trips':0,'policy':'ATM only for diagnostic active graph; direct feed separate snapshot; no label-based merge'}
    return {'feeds':reports,'reconciliation':reconciliation,'canonical_choice':'diagnostic ATM snapshot only, not approved production source','double_counted_standalone_trips':0,'actual_traffic_verified':False}
def y9():
    out={}
    for ds in ['7bvh-jvq2','nzvn-apee']:
        # Whole lifetime inventory: materially broader query than the earlier 2021 test.
        p={'$where':"codi_estacio='Y9'",'$select':'count(*) as n,min(data_lectura) as first,max(data_lectura) as last'}
        u=BASE+'/resource/'+ds+'.json?'+urllib.parse.urlencode(p)
        try:
            with urllib.request.urlopen(u,timeout=30) as r:
                b=r.read(65537);req(len(b)<=65536,'response cap');req(r.status==200,'http')
            obj=json.loads(b);dest=OUT/('Y9_'+ds+'_lifetime.json');dest.write_bytes(b)
            out[ds]={'url':u,'sha256':sha(b),'response':obj,'status':'CHECKS_RECORDED'}
        except Exception as e:out[ds]={'url':u,'error':str(e),'status':'FAILED'}
    return {'inventory':out,'cause_confirmed':False,'no_imputation':True}
def main():
    def safe(t):
        k,f=t
        try:return k,{'status':'CHECKS_RECORDED','data':f()}
        except Exception as e:return k,{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=2) as ex:r=dict(ex.map(safe,[('transport',transport),('Y9',y9)]))
    r.update(canonical_EAM_closed=0,production='NO_GO')
    (OUT/'report.json').write_text(json.dumps(r,indent=2,ensure_ascii=False,allow_nan=False)+'\n')
    (OUT/'README.md').write_text('DM-20 derived diagnostic graph, ATM/NAP via Mobility Database snapshot of 17/11/2026; Powered by MITRAMS https://www.transportes.gob.es/ ; https://nap.transportes.gob.es/licencia-datos . FGC and TRAM original feeds remain separate. Graph edges derived from consecutive stops, not new geographic geometry. XEMA Meteocat / Generalitat de Catalunya. No personal data, no synthetic values, NO-GO.\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error')} for k,v in r.items() if isinstance(v,dict)},ensure_ascii=False))
if __name__=='__main__':main()
