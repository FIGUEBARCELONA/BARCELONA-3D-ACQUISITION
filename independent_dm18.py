"""DM18: actual bounded open-data acquisition. No imputation or homologation."""
import csv, gzip, hashlib, io, json, math, re, zipfile, urllib.request, urllib.parse, datetime as dt, collections
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
OUT=Path('custody/dm18'); OUT.mkdir(parents=True,exist_ok=True)
OLD=Path('custody/dm17')
BASE='https://analisi.transparenciacatalunya.cat'
ST=['D5','X2','X4','X8','XL','Y7','Y9','YQ']
def check(b,m):
    if not b: raise ValueError(m)
def sha(b):return hashlib.sha256(b).hexdigest()
def get(u,p=None,cap=16*1024**2):
    if p:u+='?'+urllib.parse.urlencode(p)
    with urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'Barcelona3D public data QA','Accept-Encoding':'identity'}),timeout=45) as r:
        b=r.read(cap+1);check(len(b)<=cap,'cap exceeded');check(r.status==200,'HTTP status')
        return b,{'url':r.geturl(),'bytes':len(b),'sha256':sha(b),'content_type':r.headers.get('Content-Type')}
def save(n,b,m,compress=True):
    if compress:n+='.gz'; c=gzip.compress(b,mtime=0);check(gzip.decompress(c)==b,'gzip roundtrip')
    else:c=b
    p=OUT/n;check(not p.exists() or p.read_bytes()==c,'immutable collision');p.write_bytes(c)
    return dict(m,path=str(p),stored_sha256=sha(c),stored_bytes=len(c))
def load(p):return json.loads(gzip.decompress(p.read_bytes()))
def daily():
    r=json.loads((OLD/'report.json').read_text());vm={str(x['codi_variable']):x for x in load(OLD/'variables.json.gz')}
    ct=collections.Counter();issues=collections.Counter();flags=collections.Counter();units=collections.Counter();ids=set()
    for q in r['station_years']:
        if q['status']!='CHECKS_RECORDED':continue
        p=Path(q['source']['path']);b=p.read_bytes();raw=gzip.decompress(b)
        check(sha(b)==q['source']['compressed_sha256'] and sha(raw)==q['source']['sha256'],'custody hash')
        rows=json.loads(raw);check(len(rows)==q['rows'],'row count');ct['lots']+=1
        for x in rows:
            ct['rows']+=1; v=str(x['codi_variable']);u=x.get('unitat');expected=vm[v]['unitat'];units[(v,str(u),str(expected))]+=1
            if u!=expected:issues['unit_mismatch']+=1
            k=x.get('id')
            if k is None:issues['missing_id']+=1
            elif k in ids:issues['duplicate_id']+=1
            ids.add(k);flags[x.get('estat','ABSENT')]+=1
    return {'counts':dict(ct),'issues':dict(issues),'flags':dict(flags),'units':[{'variable':a,'observed':b,'metadata':c,'rows':n} for (a,b,c),n in sorted(units.items())]}
def subdaily():
    b,m=get(BASE+'/api/views/nzvn-apee.json');meta=json.loads(b)
    fields={c['fieldName']:c.get('description') for c in meta['columns'] if not c['fieldName'].startswith(':')}
    src=save('xema_subdaily_metadata.json',b,m)
    vm={str(x['codi_variable']):x for x in load(OLD/'variables.json.gz')}
    for f in ['codi_estacio','codi_variable','data_lectura','valor']:check(f in fields,'missing '+f)
    base=next((k for k in ['base_horaria','base_horaria_lectura','base_temporal'] if k in fields),None)
    flag=next((k for k in ['codi_estat','estat','estat_lectura'] if k in fields),None)
    # Month deliberately limited to seven active January 2025 stations (X2 and Y9 instead January 2021).
    periods={s:('2021-01-01','2021-02-01') if s in ('X2','Y9') else ('2025-01-01','2025-02-01') for s in ST}
    def one(s):
        a,z=periods[s];where=f"codi_estacio='{s}' AND data_lectura>='{a}T00:00:00' AND data_lectura<'{z}T00:00:00'"
        try:
            raw,mm=get(BASE+'/resource/nzvn-apee.json',{'$where':where,'$limit':100000,'$order':'data_lectura,codi_variable,:id'},32*1024**2)
            rs=json.loads(raw);check(isinstance(rs,list) and len(rs)<100000,'truncated')
            cnt,_=get(BASE+'/resource/nzvn-apee.json',{'$where':where,'$select':'count(*) as n'})
            check(int(json.loads(cnt)[0]['n'])==len(rs),'count mismatch')
            source=save('subdaily_'+s+'_'+a[:7]+'.json',raw,mm)
            seen=collections.Counter(); quality=collections.Counter();variables=collections.Counter();issue=collections.Counter();intervals=collections.defaultdict(list)
            for x in rs:
                v=str(x['codi_variable']); t=x['data_lectura']; ba=x.get(base,'ABSENT') if base else 'ABSENT'
                check(x['codi_estacio']==s and a<=t[:10]<z,'out of scope')
                seen[(v,t,ba)]+=1;variables[v]+=1;quality[str(x.get(flag,'ABSENT')) if flag else 'ABSENT']+=1
                if v not in vm:issue['unknown_variable']+=1
                if 'unitat' in fields and v in vm and x.get('unitat')!=vm[v]['unitat']:issue['unit_mismatch']+=1
                try:
                    if not math.isfinite(float(x['valor'])):issue['nonfinite']+=1
                except (ValueError,TypeError,KeyError):issue['missing_or_invalid']+=1
                intervals[(v,ba)].append(t)
            issue['duplicate_keys']=sum(n-1 for n in seen.values())
            # Observed range != lifecycle of physical sensor.
            observed=[{'variable':v,'base':ba,'first':min(ts),'last':max(ts),'rows':len(ts),'distinct_times':len(set(ts))} for (v,ba),ts in sorted(intervals.items())]
            return {'station':s,'period':[a,z],'rows':len(rs),'source':source,'status':'CHECKS_RECORDED','flags':dict(quality),'issues':dict(issue),'observed_support':observed}
        except Exception as e:return {'station':s,'status':'FAILED','error':str(e),'rows':0}
    with ThreadPoolExecutor(max_workers=3) as ex: lots=list(ex.map(one,ST))
    return {'metadata':src,'fields':fields,'time_base_field':base,'quality_field':flag,'lots':lots,'rows':sum(x['rows'] for x in lots),'sensor_lifecycle_verified':False}
def gtfs():
    raw,m=get('https://www.fgc.cat/google/google_transit.zip',cap=32*1024**2)
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        check(sum(i.file_size for i in z.infolist())<160*1024**2,'expanded cap')
        check(z.testzip() is None,'CRC')
        names=z.namelist(); tables={}
        for name in ['agency','stops','routes','trips','stop_times','calendar','calendar_dates','shapes','feed_info']:
            f=name+'.txt'
            if f in names:
                with z.open(f) as b:tables[name]=list(csv.DictReader(io.TextIOWrapper(b,encoding='utf-8-sig')))
        for n in ['agency','stops','routes','trips','stop_times']:check(n in tables and tables[n],'missing '+n)
        ids={};dups={}
        for n,k in [('stops','stop_id'),('routes','route_id'),('trips','trip_id')]:
            v=[x[k] for x in tables[n]];ids[n]=set(v);dups[n]=len(v)-len(ids[n])
        services={x['service_id'] for n in ('calendar','calendar_dates') for x in tables.get(n,[])}
        shapes={x['shape_id'] for x in tables.get('shapes',[])}
        fk={'trip_route':sum(x['route_id'] not in ids['routes'] for x in tables['trips']),'trip_service':sum(x['service_id'] not in services for x in tables['trips']),'stop_time_trip':sum(x['trip_id'] not in ids['trips'] for x in tables['stop_times']),'stop_time_stop':sum(x['stop_id'] not in ids['stops'] for x in tables['stop_times']),'trip_shape':sum(bool(x.get('shape_id')) and x['shape_id'] not in shapes for x in tables['trips'])}
        def seconds(s):
            h,m,se=map(int,s.split(':'));check(h>=0 and 0<=m<60 and 0<=se<60,'bad GTFS time');return h*3600+m*60+se
        grouped=collections.defaultdict(list);invalid=0;overnight=0
        for x in tables['stop_times']:
            try:
                a=seconds(x['arrival_time']);b=seconds(x['departure_time']);s=int(x['stop_sequence'])
                invalid+=b<a;overnight+=a>=86400 or b>=86400;grouped[x['trip_id']].append((s,a,b))
            except Exception:invalid+=1
        dupseq=reverse=0;edges=set()
        for trip,rs in grouped.items():
            rs.sort();dupseq+=len(rs)-len({a[0] for a in rs});reverse+=sum(x[2]>y[1] for x,y in zip(rs,rs[1:]))
        dates=[x[k] for x in tables.get('calendar',[]) for k in ['start_date','end_date']]+[x['date'] for x in tables.get('calendar_dates',[])]
        source=save('FGC_GTFS.zip',raw,m,False)
        return {'source':source,'members':names,'counts':{k:len(v) for k,v in tables.items()},'duplicate_ids':dups,'foreign_key_errors':fk,'invalid_times':invalid,'duplicate_sequences':dupseq,'nonmonotonic_adjacent_times':reverse,'stop_times_after_midnight':overnight,'calendar_extremes':[min(dates),max(dates)] if dates else None,'agency_timezone':sorted({x['agency_timezone'] for x in tables['agency']}),'actual_occupancy_verified':False,'scope':'FGC entire feed, not all Barcelona mobility'}
def population():
    raw,m=get('https://opendata-ajuntament.barcelona.cat/data/dataset/d978cd89-53f7-41be-b4ba-91a0b5d36f71/resource/672cde4d-9900-491e-8bc4-adeeae33fe14/download')
    s=raw.decode('utf-8-sig');dialect=csv.Sniffer().sniff(s[:4000],delimiters=',;');reader=csv.DictReader(io.StringIO(s),dialect=dialect)
    rows=list(reader);check(len(rows)>0,'empty CSV')
    fields=reader.fieldnames;source=save('BCN_padro_2025.csv',raw,m)
    # Discover actual column names; only aggregate when numeric population column and geography explicitly identified.
    def field(*names):return next((x for x in fields if x.lower() in names),None)
    n=field('nombre','valor','poblacio','població','persones');d=field('codi_districte');b=field('codi_barri');yr=field('any');sex=field('sexe');age=field('edat','edat_1','edat_quinquennal');section=field('seccio_censal','codi_seccio_censal','secció_censal')
    ret={'source':source,'columns':fields,'rows':len(rows),'mapping':{'value':n,'district':d,'barri':b,'year':yr,'sex':sex,'age':age,'section':section},'examples':rows[:3]}
    if n and d and b:
        ds=collections.Counter();bs=collections.Counter();bad=0;neg=0;years=set();sexes=set();ages=set();unique=set();dup=0
        for x in rows:
            if yr:years.add(x[yr])
            if sex:sexes.add(x[sex])
            if age:ages.add(x[age])
            k=tuple(x[k] for k in fields if k!=n)
            dup+=k in unique;unique.add(k)
            try:
                v=int(x[n]);neg+=v<0;ds[x[d]]+=v;bs[x[b]]+=v
            except (ValueError,TypeError):bad+=1
        ret.update(raw_numeric_sum=sum(ds.values()),district_sums=dict(ds),barri_sums=dict(bs),nonnumeric=bad,negative=neg,duplicate_full_dimensions=dup,years=sorted(years),sex_categories=sorted(sexes),age_categories=sorted(ages),aggregation_requires_no_subtotal_verification=True)
    return ret
def main():
    def safe(q):
        k,f=q
        try:return k,{'status':'CHECKS_RECORDED','data':f()}
        except Exception as e:return k,{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=4) as ex:r=dict(ex.map(safe,[('daily_readback',daily),('subdaily',subdaily),('FGC',gtfs),('population',population)]))
    r.update(canonical_EAM_closed=0,production='NO_GO',email_submitted=False)
    (OUT/'report.json').write_text(json.dumps(r,indent=2,ensure_ascii=False,allow_nan=False)+'\n')
    (OUT/'README.md').write_text('DM-18. Fonts originals públiques: Meteocat XEMA nzvn-apee/4fb2-n3yi; FGC https://www.fgc.cat/google/google_transit.zip; Ajuntament de Barcelona, Padró 2025. Atribució: Generalitat de Catalunya, Servei Meteorològic de Catalunya, FGC, Ajuntament de Barcelona. URL, hash i abast a report.json. Respostes originals gzip sense pèrdua. Períodes diagnòstics, no data de tall aprovada. No imputació, NO-GO.\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error')} for k,v in r.items() if isinstance(v,dict)},ensure_ascii=False))
if __name__=='__main__':main()
