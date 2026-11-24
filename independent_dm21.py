"""DM21: independent source checks, reversible diagnostic exclusion, FGC schedule signatures."""
import independent_dm20d as d
import independent_dm20c as c
import collections as C, json, math, csv, gzip, io, hashlib, urllib.request, re, unicodedata
from pathlib import Path
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor
O=Path('custody/dm21');O.mkdir(parents=True,exist_ok=True)
def req(x,m):
    if not x:raise ValueError(m)
def save(name,raw):
    p=O/name;p.write_bytes(raw);return {'path':str(p),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
def get(u,cap=48*1024**2):
    with urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'Barcelona3D open data QA','Accept-Encoding':'identity'}),timeout=40) as r:
        b=r.read(cap+1);req(len(b)<=cap,'cap');req(r.status==200,'HTTP')
    return b
def norm(s):
    return ' '.join(unicodedata.normalize('NFKD',s).casefold().replace('’',"'").replace('-',' ').split())
def dist(a,b):
    p,q=map(math.radians,[float(a['stop_lat']),float(b['stop_lat'])]);dl=math.radians(float(a['stop_lon'])-float(b['stop_lon']))
    h=math.sin((p-q)/2)**2+math.cos(p)*math.cos(q)*math.sin(dl/2)**2
    return 6371008.8*2*math.asin(min(1,math.sqrt(h)))
def fgc():
    a=d.audit(d.sources[0][1],d.sources[0][2]);b=d.audit(d.sources[1][1],d.sources[1][2])
    def signatures(x):
        bag=C.defaultdict(list)
        for t,rs in x['active_records'].items():
            if x['op'][t]!='FGC':continue
            rr=sorted(rs);label=x['route'][x['trips'][t]['route_id']][1]
            key=(label,tuple((r[2],r[3]) for r in rr))
            bag[key].append((t,[r[1] for r in rr]))
        return bag
    aa,bb=signatures(a),signatures(b);votes=C.defaultdict(C.Counter);rev=C.defaultdict(C.Counter);pairs=0
    for k in aa.keys()&bb.keys():
        if len(aa[k])==len(bb[k])==1:
            pairs+=1
            for x,y in zip(bb[k][0][1],aa[k][0][1]):votes[x][y]+=1;rev[y][x]+=1
    mapping={s:next(iter(v)) for s,v in votes.items() if len(v)==1 and len(rev[next(iter(v))])==1}
    rows=[]
    for s,t in sorted(mapping.items()):
        x,y=b['stops'][s],a['stops'][t]
        rows.append({'direct':s,'atm':t,'direct_name':x['stop_name'],'atm_name':y['stop_name'],'name_exact_normalized':norm(x['stop_name'])==norm(y['stop_name']),'distance_m':dist(x,y),'support_unique_trip_pairs':votes[s][t],'direct_coords':[x['stop_lon'],x['stop_lat']],'atm_coords':[y['stop_lon'],y['stop_lat']]})
    def fp(x,mp=None):
        bag=C.Counter();edges=set();unmapped=0
        for t,rs in x['active_records'].items():
            if x['op'][t]!='FGC':continue
            rr=sorted(rs);ids=[r[1] for r in rr]
            if mp is not None:
                if not all(s in mp for s in ids):unmapped+=1;continue
                ids=[mp[s] for s in ids]
            label=x['route'][x['trips'][t]['route_id']][1]
            bag[label,tuple((s,r[2],r[3]) for s,r in zip(ids,rr))]+=1
            edges.update((label,s,v) for s,v in zip(ids,ids[1:]))
        return bag,edges,unmapped
    ap,ae,_=fp(a);bp,be,u=fp(b,mapping)
    return {'unique_schedule_pairs':pairs,'active_stops_direct':len(b['stop_ids_by_op']['FGC']),'unambiguous_sequence_candidates':len(mapping),'exact_normalized_names':sum(r['name_exact_normalized'] for r in rows),'distance_bands_m':{str(t):sum(r['distance_m']<=t for r in rows) for t in [0.01,1,5,10,25,50,100]},'max_distance_m':max((r['distance_m'] for r in rows),default=None),'trip_multiset_matches':sum((ap&bp).values()),'edge_matches':len(ae&be),'unmapped_trips':u,'crosswalk':rows,'physical_identity_approved':False,'merged':0,'method':'Unique route label plus full ordered time sequence, mutual unambiguous votes; spatial distance diagnostic, no threshold for production.'}
def temporal():
    with c.acquire(d.sources[0][1],d.sources[0][2]) as z:
        agencies={x.get('agency_id',''):x['agency_name'] for x in c.rows(z,'agency')};routes={x['route_id']:agencies[x.get('agency_id','')] for x in c.rows(z,'routes')};trips={x['trip_id']:x for x in c.rows(z,'trips')}
        active={x['service_id'] for x in c.rows(z,'calendar') if c.date(x['start_date'])<=c.DAY<=c.date(x['end_date']) and x[c.DAY.strftime('%A').lower()]=='1'}
        for x in c.rows(z,'calendar_dates'):
            if c.date(x['date'])==c.DAY:
                if x['exception_type']=='1':active.add(x['service_id'])
                else:active.discard(x['service_id'])
        ids={t for t,x in trips.items() if x['service_id'] in active};groups=C.defaultdict(list)
        for x in c.rows(z,'stop_times'):
            t=x['trip_id']
            if t in ids:groups[t].append((int(x['stop_sequence']),x['stop_id'],x.get('arrival_time',''),x.get('departure_time','')))
        counts=C.Counter();opcounts=C.defaultdict(C.Counter);missingrows=C.Counter();exclusions=[];eligible=[]
        for t in sorted(ids):
            rr=sorted(groups[t]);op=routes[trips[t]['route_id']];missing=sum(not x[2] or not x[3] for x in rr)
            if len(rr)<2:reason='FEWER_THAN_TWO_STOPS'
            elif missing==len(rr):reason='ALL_TIMES_ABSENT'
            elif missing and (not rr[0][2] or not rr[0][3] or not rr[-1][2] or not rr[-1][3]):reason='ENDPOINT_TIME_ABSENT'
            elif missing:reason='INTERMEDIATE_TIMES_ABSENT'
            else:reason='COMPLETE_TIME_FIELDS'
            counts[reason]+=1;opcounts[op][reason]+=1;missingrows[reason]+=missing
            if reason!='COMPLETE_TIME_FIELDS':exclusions.append({'trip_id':t,'operator':op,'reason':reason,'stop_rows':len(rr),'missing_rows':missing})
            else:eligible.append(t)
        req(sum(counts.values())==64034,'active count changed');req(sum(missingrows.values())==285726,'missing row count')
        p=save('atm_active_temporal_exclusions.json.gz',gzip.compress(json.dumps(exclusions,ensure_ascii=False,separators=(',',':')).encode(),mtime=0))
        return {'active_templates':len(ids),'classes':dict(counts),'missing_rows_by_class':dict(missingrows),'by_operator':{k:dict(v) for k,v in opcounts.items()},'quarantined_templates':len(exclusions),'complete_field_templates':len(eligible),'exclusions':p,'original_changed':False,'imputed_times':0,'policy':'Diagnostic strict timed subset, not a decision that intermediate blank times are invalid; no restoration of missing source.'}
class Table(HTMLParser):
    def __init__(self):super().__init__(convert_charrefs=True);self.tables=[];self.table=None;self.row=None;self.cell=None
    def handle_starttag(self,t,a):
        if t=='table':self.table=[]
        elif t=='tr' and self.table is not None:self.row=[]
        elif t in ('td','th') and self.row is not None:self.cell=[]
    def handle_data(self,s):
        if self.cell is not None:self.cell.append(s)
    def handle_endtag(self,t):
        if t in ('td','th') and self.cell is not None:
            self.row.append(' '.join(''.join(self.cell).split()));self.cell=None
        elif t=='tr' and self.row is not None:self.table.append(self.row);self.row=None
        elif t=='table' and self.table is not None:self.tables.append(self.table);self.table=None
def population():
    # The public statistical table is a distinct primary distribution from the blocked CSV.
    u='https://portaldades.ajuntament.barcelona.cat/ca/estad%C3%ADstiques/yzlntdm2fs'
    b=get(u);p=Table();p.feed(b.decode('utf-8'))
    out={'url':u,'html_sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b),'tables':[{'rows':len(t),'first':t[:3]} for t in p.tables],'status':'INSPECTED_NOT_VALIDATED'}
    for i,t in enumerate(p.tables):
        if len(t)>=74 and any('2025' in x for r in t[:4] for x in r):
            out.setdefault('candidates',[]).append(save('population_table_'+str(i)+'.json',json.dumps(t,ensure_ascii=False).encode()))
    return out
def geometry():
    u='https://geoportal.barcelona.cat/geoBCN/serveis/territori/barris/?extensio=true&geometria=true&out_proj=EPSG:25831'
    b=get(u,12*1024**2);j=json.loads(b);src=save('geobcn_barris.json.gz',gzip.compress(b,mtime=0))
    return {'url':u,'raw_sha256':hashlib.sha256(b).hexdigest(),'source':src,'type':type(j).__name__,'count':len(j),'first':str(j[0] if isinstance(j,list) and j else j)[:1800],'geometry_validation_pending':True}
def main():
    def safe(p):
        k,f=p
        try:return k,{'status':'CHECKS_RECORDED','data':f()}
        except Exception as e:return k,{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=4) as pool:r=dict(pool.map(safe,[('FGC',fgc),('ATM',temporal),('population',population),('geometry',geometry)]))
    r.update(canonical_EAM_closed=0,production='NO_GO')
    (O/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    (O/'README.md').write_text('DM-21. Source GTFS ATM via Mobility Database/NAP: Powered by MITRAMS https://www.transportes.gob.es/ ; FGC original. OMD and GeoBCN: Ajuntament de Barcelona. Diagnostic candidate matches and exclusions only, no modification of originals or imputation. NO-GO.\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error')} for k,v in r.items() if isinstance(v,dict)}))
if __name__=='__main__':main()
