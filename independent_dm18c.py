"""DM-18c: readback, exact time-base validation and census reconciliation.
No new data fabrication; retry only X4 by seven-day slices and inspect Y9 inventory."""
import csv,gzip,hashlib,io,json,math,datetime as dt,collections
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import independent_dm18b as common
ROOT=Path('custody/dm18c');ROOT.mkdir(parents=True,exist_ok=True)
OLD=Path('custody/dm18b'); BASE=common.BASE; common.OUT=ROOT
def check(x,m):
    if not x: raise ValueError(m)
def read(src):
    b=Path(src['path']).read_bytes();check(common.sha(b)==src['stored_sha256'],'stored hash')
    raw=gzip.decompress(b) if src['path'].endswith('.gz') else b
    check(common.sha(raw)==src['sha256'],'original hash');return raw
def population():
    r=json.loads((OLD/'report.json').read_text())['population']['data']
    raw=read(r['source']); rows=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig')),delimiter=';'))
    selected=[x for x in rows if x['municipi']=='Barcelona' and x['any']=='2025' and x['concepte']=='població']
    check(len(selected)==66,'Barcelona row count')
    check(not any(x['estat'] for x in selected),'flagged values')
    data={}
    for x in selected:
        k=x['edat'],x['sexe'];check(k not in data,'duplicate age sex');data[k]=int(x['valor'])
    ages=[a for a,s in data if s=='total' and a!='total']
    check(len(ages)==21,'age group count')
    for a in ages+['total']:check(data[a,'homes']+data[a,'dones']==data[a,'total'],'sex reconciliation '+a)
    for s in ['homes','dones','total']:check(sum(data[a,s] for a in ages)==data['total',s],'age reconciliation '+s)
    check(data['total','total']==1713247,'Idescat municipal total cross-check')
    return {'rows':len(selected),'ages':len(ages),'sex_total_checks':22,'age_total_checks':3,'totals':{s:data['total',s] for s in ['homes','dones','total']},'groups':[{'age':a,**{s:data[a,s] for s in ['homes','dones','total']}} for a in ages],'source':r['source'],'city_scope':'Barcelona municipality only, annual census 1 January 2025; not padró nor street presence'}
def inspect(rows,station,a,z):
    seen=collections.Counter();base=collections.Counter();flags=collections.Counter();errors=collections.Counter();variables=collections.Counter();vm={str(x['codi_variable']):x for x in json.loads(gzip.decompress(Path('custody/dm17/variables.json.gz').read_bytes()))}
    supports=collections.defaultdict(list)
    for x in rows:
        v=str(x['codi_variable']);t=x['data_lectura'];ba=x.get('codi_base','ABSENT');fl=x.get('codi_estat','ABSENT')
        check(x['codi_estacio']==station and a<=t[:10]<z,'out of scope')
        seen[v,t,ba]+=1;base[ba]+=1;flags[fl]+=1;variables[v]+=1;supports[v,ba].append(t)
        if ba not in ('HO','SH'):errors['unexpected_base']+=1
        ts=dt.datetime.fromisoformat(t.replace('Z','+00:00'))
        if ts.second or ts.microsecond or (ba=='HO' and ts.minute!=0) or (ba=='SH' and ts.minute not in (0,30)):errors['unaligned_start']+=1
        if v not in vm:errors['unknown_variable']+=1
        try:
            if not math.isfinite(float(x['valor_lectura'])):errors['nonfinite']+=1
        except (TypeError,ValueError,KeyError):errors['missing_invalid_value']+=1
    errors['duplicate_keys']=sum(n-1 for n in seen.values())
    return {'station':station,'period':[a,z],'rows':len(rows),'bases':dict(base),'flags':dict(flags),'errors':dict(errors),'variables':dict(variables),'observed_support':[{'variable':v,'base':ba,'first':min(ts),'last':max(ts),'rows':len(ts),'unique_times':len(set(ts)),'metadata_unit':vm.get(v,{}).get('unitat')} for (v,ba),ts in sorted(supports.items())]}
def previous():
    r=json.loads((OLD/'report.json').read_text())['subdaily']['data'];lots=[]
    for x in r['lots']:
        if x['status']=='CHECKS_RECORDED':
            raw=read(x['source']);rows=json.loads(raw);check(len(rows)==x['rows'],'count')
            lots.append(inspect(rows,x['station'],*x['period']))
    return {'lots':lots,'rows':sum(q['rows'] for q in lots),'custody_lots':len(lots),'sensor_lifecycle_verified':False}
def fetch(st,a,z):
    where=f"codi_estacio='{st}' AND data_lectura>='{a}T00:00:00' AND data_lectura<'{z}T00:00:00'"
    raw,m=common.get(BASE+'/resource/nzvn-apee.json',{'$where':where,'$limit':25000,'$order':'data_lectura,codi_variable,:id'})
    rs=json.loads(raw);check(isinstance(rs,list) and len(rs)<25000,'truncated')
    count,_=common.get(BASE+'/resource/nzvn-apee.json',{'$where':where,'$select':'count(*) as n'})
    check(int(json.loads(count)[0]['n'])==len(rs),'count mismatch')
    source=common.save('subdaily_'+st+'_'+a+'.json',raw,m);return dict(inspect(rs,st,a,z),source=source)
def x4():
    dates=['2025-01-01','2025-01-08','2025-01-15','2025-01-22','2025-01-29','2025-02-01']
    def one(p):
        try:return dict(fetch('X4',*p),status='CHECKS_RECORDED')
        except Exception as e:return {'period':list(p),'status':'FAILED','error':str(e),'rows':0}
    with ThreadPoolExecutor(max_workers=2) as ex:lots=list(ex.map(one,zip(dates,dates[1:])))
    return {'lots':lots,'rows':sum(x['rows'] for x in lots)}
def y9():
    # Count inventory in the known operational year, without guessing new observations.
    where="codi_estacio='Y9' AND data_lectura>='2021-01-01T00:00:00' AND data_lectura<'2022-01-01T00:00:00'"
    raw,m=common.get(BASE+'/resource/nzvn-apee.json',{'$where':where,'$select':'count(*) as n,min(data_lectura) as first,max(data_lectura) as last'})
    rows=json.loads(raw);source=common.save('Y9_2021_count.json',raw,m)
    return {'inventory':rows,'source':source,'no_imputation':True}
def main():
    def safe(q):
        k,f=q
        try:return k,{'status':'CHECKS_RECORDED','data':f()}
        except Exception as e:return k,{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=4) as ex:r=dict(ex.map(safe,[('population',population),('subdaily_readback',previous),('X4_recovery',x4),('Y9_inventory',y9)]))
    r.update(canonical_EAM_closed=0,production='NO_GO')
    (ROOT/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    (ROOT/'README.md').write_text('DM-18c. Font original: Meteocat, Generalitat de Catalunya, XEMA nzvn-apee; Idescat, Cens anual 2025. Atribució i URL a informes DM-17/18b. Sense imputació. Cap equivalència amb vigència física del sensor, padró ni flux de persones. NO-GO.\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error'),'rows':v.get('data',{}).get('rows')} for k,v in r.items() if isinstance(v,dict)},ensure_ascii=False))
if __name__=='__main__':main()
