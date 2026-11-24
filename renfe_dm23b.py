"""DM23b: preserve direct Renfe original before parsing; inspect exact calendar schema."""
import sources_dm23 as c
import io,zipfile,csv,json,collections,hashlib
from pathlib import Path
O=Path('custody/dm23b');O.mkdir(parents=True,exist_ok=True)
def main():
    u='https://ssl.renfe.com/ftransit/Fichero_CER_FOMENTO/fomento_transit.zip'
    raw,m=c.get(u,32*1024**2)
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        c.req(sum(x.file_size for x in z.infolist())<350*1024**2,'expanded cap');c.req(z.testzip() is None,'CRC')
        p=O/'renfe_cercanias.zip';p.write_bytes(raw)
        result={'source':m,'custody':{'path':str(p),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()},'crc_verified':True,'members':z.namelist()}
        tables={};schemas={}
        for n in ['agency','routes','trips','calendar','calendar_dates','stop_times','feed_info']:
            if n+'.txt' not in z.namelist():continue
            with z.open(n+'.txt') as f:
                reader=csv.DictReader(io.TextIOWrapper(f,encoding='utf-8-sig'))
                fields=reader.fieldnames or [];schemas[n]=fields
                rs=[]
                for i,x in enumerate(reader):
                    # For stop_times, stream below. Preserve source headers exactly; normalize only whitespace around labels.
                    if n=='stop_times' and i>=2:break
                    if None in x:raise ValueError('row shape '+n)
                    rs.append({k.strip():v for k,v in x.items()})
                tables[n]=rs
        result['schemas']=schemas;result['sample_calendar']=tables.get('calendar',[])[:4];result['sample_calendar_dates']=tables.get('calendar_dates',[])[:4];result['sample_trip_ids']=[x.get('trip_id') for x in tables['trips'][:15]]
        old=json.loads(Path('custody/dm22b/report.json').read_text())['ATM_full']['data']['target_rows'];targets={x['trip_id'] for x in old if x['simplified_active'] and x['simplified_operator']=='RENFE OPERADORA'}
        trips={x['trip_id']:x for x in tables['trips']};match={t:[k for k in [t,t.removeprefix('REG_B_')] if k in trips] for t in sorted(targets)}
        ids={i for v in match.values() for i in v};groups=collections.defaultdict(list)
        with z.open('stop_times.txt') as f:
            for x in csv.DictReader(io.TextIOWrapper(f,encoding='utf-8-sig')):
                x={k.strip():v for k,v in x.items()}
                if x['trip_id'] in ids:groups[x['trip_id']].append(x)
        active=set();calendar_status='NOT_VERIFIED'
        try:
            gt=c.gt;wd=gt.DAY.strftime('%A').lower()
            for x in tables.get('calendar',[]):
                if gt.date(x['start_date'])<=gt.DAY<=gt.date(x['end_date']) and x[wd]=='1':active.add(x['service_id'])
            for x in tables.get('calendar_dates',[]):
                if gt.date(x['date'])==gt.DAY:
                    if x['exception_type']=='1':active.add(x['service_id'])
                    elif x['exception_type']=='2':active.discard(x['service_id'])
                    else:raise ValueError('exception type')
            calendar_status='CHECKS_RECORDED'
        except Exception as e:calendar_status='FAILED: '+str(e)
        result.update(target_count=len(targets),trip_count=len(trips),calendar_status=calendar_status,results=[{'target':t,'candidates':[{'trip_id':i,'active':trips[i]['service_id'] in active if calendar_status=='CHECKS_RECORDED' else None,'stop_rows':len(groups[i]),'complete_time_rows':sum(bool(x.get('arrival_time')) and bool(x.get('departure_time')) for x in groups[i])} for i in m]} for t,m in match.items()],candidate_count=sum(bool(v) for v in match.values()),recovered_exact_service_identity=0)
        return result
try:r={'status':'CHECKS_RECORDED','data':main()}
except Exception as e:r={'status':'FAILED','error':str(e)}
r.update(production='NO_GO',canonical_EAM_closed=0)
(O/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'status':r['status'],'error':r.get('error'),'candidate_count':r.get('data',{}).get('candidate_count')}))
