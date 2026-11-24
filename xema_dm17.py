#!/usr/bin/env python3
"""DM-17: official XEMA daily observations; diagnostic 2021–2025, no imputation."""
import urllib.request, urllib.parse, json, hashlib, gzip, datetime as dt, math, re, collections
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
BASE='https://analisi.transparenciacatalunya.cat'
OUT=Path('custody/dm17');OUT.mkdir(parents=True,exist_ok=True)
START=dt.date(2021,1,1);END=dt.date(2026,1,1)
STATIONS={'D5':('1995-11-03',None),'X2':('2006-09-22','2024-10-03'),'X4':('2006-10-11',None),'X8':('2008-04-17',None),'XL':('2011-03-22',None),'Y7':('2015-04-08',None),'Y9':('2015-06-26','2021-11-30'),'YQ':('2021-12-02',None)}
def check(x,m):
    if not x:raise ValueError(m)
def get(path,params=None,cap=12*1024*1024):
    url=BASE+path+('?' + urllib.parse.urlencode(params) if params else '')
    req=urllib.request.Request(url,headers={'User-Agent':'Barcelona3D public data reproducibility QA','Accept':'application/json','Accept-Encoding':'identity'})
    with urllib.request.urlopen(req,timeout=45) as r:
        raw=r.read(cap+1)
        check(len(raw)<=cap,'response cap exceeded')
        check(r.status==200,'HTTP '+str(r.status))
    obj=json.loads(raw)
    return raw,obj,url
def preserve(name,raw,url):
    p=OUT/(name+'.json.gz');compressed=gzip.compress(raw,compresslevel=9,mtime=0)
    check(not p.exists() or p.read_bytes()==compressed,'immutable source collision')
    p.write_bytes(compressed)
    check(gzip.decompress(p.read_bytes())==raw,'lossless gzip roundtrip')
    return {'path':str(p),'url':url,'bytes':len(raw),'compressed_bytes':len(compressed),'sha256':hashlib.sha256(raw).hexdigest(),'compressed_sha256':hashlib.sha256(compressed).hexdigest()}
def meta(id):
    raw,j,u=get('/api/views/'+id+'.json')
    cols={c['fieldName']:{'name':c.get('name'),'type':c.get('dataTypeName'),'description':c.get('description')} for c in j['columns'] if not c['fieldName'].startswith(':')}
    return {'name':j.get('name'),'license':j.get('licenseId') or j.get('metadata',{}).get('custom_fields',{}),'rowsUpdatedAt':j.get('rowsUpdatedAt'),'columns':cols,'source':preserve('metadata_'+id,raw,u)}
def col(m,candidates,required=True):
    a=[k for k in candidates if k in m['columns']]
    if required:check(len(a)>0,'missing field '+str(candidates))
    return a[0] if a else None
def main():
    report={'period':'2021-01-01 inclusive to 2026-01-01 exclusive; diagnostic, not approved','dataset':'7bvh-jvq2','station_scope':STATIONS,'canonical_EAM_closed':0,'production':'NO_GO','source_attribution':'Servei Meteorològic de Catalunya (Meteocat), Dades obertes Generalitat de Catalunya; https://administraciodigital.gencat.cat/ca/dades/dades-obertes/informacio-practica/llicencies/','station_variable_coverage':[]}
    try:
        report['metadata']={id:meta(id) for id in ['7bvh-jvq2','4fb2-n3yi','yqwd-vj5e']}
        md=report['metadata']['7bvh-jvq2']
        st=col(md,['codi_estacio','codi_ema']);va=col(md,['codi_variable','codi_var']);day=col(md,['data_lectura','data','data_mesura','data_registre']);value=col(md,['valor','valor_lectura']);flag=col(md,['codi_estat','estat','estat_lectura','codi_estat_lectura'],False)
        report['fields']={'station':st,'variable':va,'date':day,'value':value,'flag':flag}
        for id,name in [('4fb2-n3yi','variables'),('yqwd-vj5e','stations')]:
            where=None
            if name=='stations':
                sm=report['metadata'][id];skey=col(sm,['codi_estacio','codi_ema'])
                where=skey+" in ("+",".join("'"+x+"'" for x in STATIONS)+")"
            p={'$limit':5000,'$order':':id'}
            if where:p['$where']=where
            raw,j,u=get('/resource/'+id+'.json',p)
            check(isinstance(j,list) and 0<len(j)<5000,'metadata rows missing or truncated')
            report[name]={'rows':len(j),'source':preserve(name,raw,u)}
            if name=='variables':
                vm=report['metadata'][id];vk=col(vm,['codi_variable','codi_var'])
                variables={str(x[vk]):x for x in j}
            else:
                report['station_metadata_codes']=sorted({x[skey] for x in j})
                check(set(report['station_metadata_codes'])==set(STATIONS),'station metadata mismatch')
        def task(pair):
            code,year=pair
            a=max(dt.date(year,1,1),dt.date.fromisoformat(STATIONS[code][0]))
            b=min(dt.date(year+1,1,1),END)
            if STATIONS[code][1]:b=min(b,dt.date.fromisoformat(STATIONS[code][1])+dt.timedelta(days=1))
            if a>=b:return {'station':code,'year':year,'status':'OUTSIDE_REGISTERED_LIFETIME','rows':0,'candidate_days':0}
            # Entire year with all returned rows, including potential observations outside listed station life.
            where=f"{st}='{code}' AND {day}>='{year}-01-01T00:00:00' AND {day}<'{year+1}-01-01T00:00:00'"
            raw,j,u=get('/resource/7bvh-jvq2.json',{'$where':where,'$limit':25000,'$order':f'{day},{va},:id'})
            check(isinstance(j,list) and len(j)<25000,'daily data truncated')
            _,counts,_=get('/resource/7bvh-jvq2.json',{'$where':where,'$select':'count(*) as n'})
            check(len(counts)==1 and int(counts[0]['n'])==len(j),'count mismatch / source changed')
            src=preserve(f'{code}_{year}',raw,u)
            groups=collections.defaultdict(list);keys=collections.Counter();outside=0;unknown=collections.Counter()
            for x in j:
                check(x[st]==code,'wrong station')
                date=dt.date.fromisoformat(x[day][:10]);check(date.year==year,'out-of-year')
                if not a<=date<b:outside+=1
                v=str(x[va]);keys[(v,date)]+=1;groups[v].append(x)
                if v not in variables:unknown[v]+=1
            cov=[]
            for v,rs in sorted(groups.items()):
                dates={x[day][:10] for x in rs if a<=dt.date.fromisoformat(x[day][:10])<b}
                finite=0;missing=0;invalid=0;flags=collections.Counter();observations=0;outlier=[]
                for x in rs:
                    if flag:flags[str(x.get(flag,'ABSENT'))]+=1
                    if x.get(value) is None:missing+=1;continue
                    try:
                        n=float(x[value])
                        if math.isfinite(n):finite+=1
                        else:invalid+=1
                    except (ValueError,TypeError):invalid+=1
                # Candidate civil days are NOT assumed active sensor days. Coverage diagnostic only.
                cov.append({'variable':v,'metadata':variables.get(v),'rows':len(rs),'distinct_days_inside_station_life':len(dates),'candidate_days':(b-a).days,'candidate_days_without_record':(b-a).days-len(dates),'finite_values':finite,'absent_values':missing,'non_numeric_or_nonfinite':invalid,'flags':dict(flags)})
            return {'station':code,'year':year,'status':'CHECKS_RECORDED','source':src,'rows':len(j),'count_confirmed':True,'candidate_days':(b-a).days,'outside_station_lifetime':outside,'duplicate_station_variable_day_extra_rows':sum(n-1 for n in keys.values() if n>1),'unknown_variable_rows':dict(unknown),'coverage':cov}
        pairs=[(s,y) for s in STATIONS for y in range(2021,2026)]
        def safe(p):
            try:return task(p)
            except Exception as e:return {'station':p[0],'year':p[1],'status':'FAILED','error':str(e),'rows':0}
        with ThreadPoolExecutor(max_workers=4) as pool:report['station_years']=list(pool.map(safe,pairs))
        report['total_rows']=sum(x['rows'] for x in report['station_years'])
        report['status']='PARTIAL_DIAGNOSTIC_NOT_HOMOLOGATED'
        report['summary']={'station_years_checked':sum(x['status']=='CHECKS_RECORDED' for x in report['station_years']),'station_years_failed':sum(x['status']=='FAILED' for x in report['station_years']),'station_years_outside_life':sum(x['status']=='OUTSIDE_REGISTERED_LIFETIME' for x in report['station_years']),'duplicate_rows':sum(x.get('duplicate_station_variable_day_extra_rows',0) for x in report['station_years']),'outside_lifetime_rows':sum(x.get('outside_station_lifetime',0) for x in report['station_years']),'unknown_variable_rows':sum(sum(x.get('unknown_variable_rows',{}).values()) for x in report['station_years']),'finite_values':sum(v['finite_values'] for x in report['station_years'] for v in x.get('coverage',[])),'null_values':sum(v['absent_values'] for x in report['station_years'] for v in x.get('coverage',[])),'nonfinite_or_nonnumeric_values':sum(v['non_numeric_or_nonfinite'] for x in report['station_years'] for v in x.get('coverage',[]))}
    except Exception as e:report['status']='FAILED';report['error']=str(e)
    (OUT/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    (OUT/'README.md').write_text('Dades originals de XEMA (Servei Meteorològic de Catalunya, Generalitat de Catalunya). Conjunts 7bvh-jvq2, 4fb2-n3yi i yqwd-vj5e. Atribució i condicions: https://administraciodigital.gencat.cat/ca/dades/dades-obertes/informacio-practica/llicencies/ . Respostes JSON originals comprimides sense pèrdua; consultes, hashes, qualitat i limitacions a report.json. No es fan interpolacions ni imputacions. Període diagnòstic no aprovat; NO-GO.\n')
    print('DM17_SUMMARY '+json.dumps({k:v for k,v in report.items() if k in ('status','error','fields','summary','total_rows','station_metadata_codes')},ensure_ascii=False))
    print('DM17_METADATA_FIELDS '+json.dumps({k:list(v['columns']) for k,v in report.get('metadata',{}).items()}))
if __name__=='__main__':main()
