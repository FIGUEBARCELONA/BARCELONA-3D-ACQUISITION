"""DM19 bounded open data QA; no silent corrections or inferred observations."""
import independent_dm18c as prev
import independent_dm18b as common
import csv, gzip, json, hashlib, io, zipfile, datetime as dt, collections, re, urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
OUT=Path('custody/dm19');OUT.mkdir(parents=True,exist_ok=True);common.OUT=OUT
def check(b,m):
    if not b:raise ValueError(m)
def normalize():
    old=json.loads(Path('custody/dm17/report.json').read_text())
    vm={str(x['codi_variable']):x for x in json.loads(gzip.decompress(Path('custody/dm17/variables.json.gz').read_bytes()))}
    patch=[];counts=collections.Counter();n=0;ids=set()
    for q in old['station_years']:
        if q['status']!='CHECKS_RECORDED':continue
        p=Path(q['source']['path']);b=p.read_bytes();raw=gzip.decompress(b)
        check(common.sha(raw)==q['source']['sha256'] and common.sha(b)==q['source']['compressed_sha256'],'source hash')
        rs=json.loads(raw);check(len(rs)==q['rows'],'row count')
        for i,x in enumerate(rs):
            n+=1;v=str(x['codi_variable']);u=x.get('unitat');expected=vm[v]['unitat']
            if u==expected:continue
            check((v in ('1000','1001','1002','1003','1004') and u=='�C' and expected=='°C') or (v in ('1509','1515') and u=='�' and expected=='°'),'unexpected difference')
            check(x['id'] not in ids,'duplicate patch id');ids.add(x['id'])
            y=dict(x,unitat=expected);z=dict(y,unitat=u);check(z==x,'reverse mismatch')
            patch.append({'source':str(p),'original_sha256':q['source']['sha256'],'row_index_zero_based':i,'id':x['id'],'station':x['codi_estacio'],'variable':v,'date':x['data_lectura'],'original_unit':u,'normalized_unit':expected,'value_unchanged':x['valor'],'reason':'exact variable dictionary match; textual encoding discrepancy only'})
            counts[v]+=1
    check(n==215058 and len(patch)==111,'unexpected counts')
    p=OUT/'unit-normalization.json';p.write_text(json.dumps(patch,ensure_ascii=False,indent=2)+'\n')
    return {'rows':n,'patch_rows':len(patch),'by_variable':dict(counts),'reverse_verified':len(patch),'originals_modified':False,'patch_sha256':common.sha(p.read_bytes()),'unit_assignment_not_calibration':True}
def recover():
    # Only previously failed X4 dates; day-scoped bounded queries.
    days=[dt.date(2025,1,1)+dt.timedelta(days=i) for i in range(14)]
    def one(a):
        z=a+dt.timedelta(days=1)
        try:return dict(prev.fetch('X4',str(a),str(z)),status='CHECKS_RECORDED')
        except Exception as e:return {'period':[str(a),str(z)],'status':'FAILED','error':str(e),'rows':0}
    with ThreadPoolExecutor(max_workers=3) as ex:lots=list(ex.map(one,days))
    return {'lots':lots,'rows':sum(x['rows'] for x in lots),'days_recovered':sum(x['status']=='CHECKS_RECORDED' for x in lots),'days_failed':sum(x['status']=='FAILED' for x in lots)}
def gtfs(name,url):
    raw,m=common.get(url,cap=32*1024**2)
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        check(sum(i.file_size for i in z.infolist())<=180*1024**2,'expanded cap')
        check(z.testzip() is None,'CRC')
        tables={}
        for t in ('agency','stops','routes','trips','stop_times','calendar','calendar_dates','shapes','feed_info'):
            if t+'.txt' in z.namelist():
                with z.open(t+'.txt') as f:tables[t]=list(csv.DictReader(io.TextIOWrapper(f,encoding='utf-8-sig')))
        check(all(tables.get(k) for k in ('agency','stops','routes','trips','stop_times')),'required tables')
        keys={};dups={}
        for t,k in [('routes','route_id'),('trips','trip_id'),('stops','stop_id')]:
            vs=[x[k] for x in tables[t]];keys[t]=set(vs);dups[t]=len(vs)-len(keys[t])
        services={x['service_id'] for t in ('calendar','calendar_dates') for x in tables.get(t,[])}
        shapes={x['shape_id'] for x in tables.get('shapes',[])}
        fk={'trip_route':sum(x['route_id'] not in keys['routes'] for x in tables['trips']),'trip_service':sum(x['service_id'] not in services for x in tables['trips']),'stop_time_trip':sum(x['trip_id'] not in keys['trips'] for x in tables['stop_times']),'stop_time_stop':sum(x['stop_id'] not in keys['stops'] for x in tables['stop_times']),'trip_shape':sum(bool(x.get('shape_id')) and x['shape_id'] not in shapes for x in tables['trips'])}
        def sec(s):
            h,m,ss=map(int,s.split(':'));check(h>=0 and 0<=m<60 and 0<=ss<60,'invalid time');return h*3600+m*60+ss
        groups=collections.defaultdict(list);invalid=overnight=0
        for x in tables['stop_times']:
            try:
                a,b=sec(x['arrival_time']),sec(x['departure_time']);invalid+=b<a;overnight+=a>=86400 or b>=86400;groups[x['trip_id']].append((int(x['stop_sequence']),a,b))
            except Exception:invalid+=1
        dupsq=reverse=0
        for rs in groups.values():
            rs.sort();dupsq+=len(rs)-len({r[0] for r in rs});reverse+=sum(x[2]>y[1] for x,y in zip(rs,rs[1:]))
        dates=[x[k] for x in tables.get('calendar',[]) for k in ('start_date','end_date')]+[x['date'] for x in tables.get('calendar_dates',[])]
        # Preserve unchanged files. NAP replica TMB explicitly attributed, no claim of latest.
        src=common.save(name+'.zip',raw,m,False)
        return {'source':src,'counts':{k:len(v) for k,v in tables.items()},'agencies':sorted({x['agency_name'] for x in tables['agency']}),'duplicate_ids':dups,'fk':fk,'invalid_times':invalid,'duplicate_sequences':dupsq,'reversed_adjacent_times':reverse,'overnight':overnight,'calendar_extremes':[min(dates),max(dates)] if dates else None,'feed_info':tables.get('feed_info',[]),'actual_occupancy_verified':False}
class Tables(HTMLParser):
    def __init__(self):super().__init__(convert_charrefs=True);self.depth=0;self.table=[];self.tables=[];self.row=None;self.cell=None
    def handle_starttag(self,t,a):
        if t=='table':
            self.depth+=1
            if self.depth==1:self.table=[]
        elif self.depth==1 and t=='tr':self.row=[]
        elif self.depth==1 and t in ('td','th'):self.cell=[]
    def handle_data(self,s):
        if self.cell is not None:self.cell.append(s)
    def handle_endtag(self,t):
        if self.depth==1 and t in ('td','th') and self.cell is not None:
            if self.row is not None:self.row.append(' '.join(''.join(self.cell).split()))
            self.cell=None
        elif self.depth==1 and t=='tr' and self.row is not None:self.table.append(self.row);self.row=None
        elif t=='table' and self.depth:
            if self.depth==1:self.tables.append(self.table)
            self.depth-=1
def population():
    url='https://dades.ajuntament.barcelona.cat/informe-padro-estrangers/ca/seccio_3.html'
    raw,m=common.get(url,cap=48*1024**2);p=Tables();p.feed(raw.decode('utf-8'))
    def num(x):return int(x.replace('.','').replace(' ',''))
    for table in p.tables:
        rows=[r for r in table if len(r)==13 and re.match(r'^\d+\.\s',r[1])]
        if len(rows)!=73:continue
        data=[]
        for r in rows:
            code=int(r[1].split('.',1)[0]);groups=[num(r[i]) for i in (2,4,6,8,10)];total=num(r[12])
            check(sum(groups)==total,'row sum mismatch '+str(code))
            data.append({'district':r[0],'barri_code':code,'barri':r[1],'total':total,'source_counts':groups})
        check({x['barri_code'] for x in data}==set(range(1,74)),'barri code coverage')
        ds=collections.Counter()
        for x in data:ds[x['district']]+=x['total']
        check(len(ds)==10,'district count')
        city=[r for r in table if len(r)==13 and r[0].upper()=='BARCELONA']
        check(len(city)==1,'city row missing')
        total=num(city[0][12]);check(sum(ds.values())==total,'city reconciliation')
        # Only factual aggregate table preserved; not full unrelated page.
        src=common.save('padro_2026_table1.json',json.dumps(table,ensure_ascii=False).encode(),{'url':m['url'],'bytes':len(raw),'sha256':common.sha(json.dumps(table,ensure_ascii=False).encode()),'html_sha256':m['sha256'],'extraction':'HTML table, not original full page'})
        return {'source':src,'date':'2026-01-01','barri_count':len(data),'district_count':len(ds),'city_total':total,'district_totals':dict(ds),'barri_totals':data,'row_sums_checked':73,'not_census':True}
    raise ValueError('73-row target table not found; table lengths '+str([len(t) for t in p.tables]))
def atm():
    url='https://t-mobilitat.cat/es/web/t-mobilitat/datos-abiertos/catalogo-de-datos/informacion-estatica'
    raw,m=common.get(url,cap=4*1024**2);s=raw.decode('utf-8','replace')
    links=re.findall(r'(?:href|src)\s*=\s*["\']([^"\']+)["\']',s,re.I)
    return {'source':m,'candidate_download_links':[urllib.parse.urljoin(url,x) for x in links if re.search(r'\.zip(?:[?#]|$)|gtfs|informacion-estatica',x,re.I)][:40],'portlet_unavailable':('no está disponible temporalmente' in s or 'no està disponible temporalment' in s),'status':'DISCOVERY_ONLY'}
def main():
    tasks=[('unit_normalization',normalize),('X4',recover),('TBX',lambda:gtfs('TRAM_TBX','https://opendata.tram.cat/GTFS/zip/TBX.zip')),('TBS',lambda:gtfs('TRAM_TBS','https://opendata.tram.cat/GTFS/zip/TBS.zip')),('TMB_NAP_replica',lambda:gtfs('TMB_NAP_replica','https://gtfs-source-feeds.transit.land/es-nap-1328.zip')),('ATM_discovery',atm),('population',population)]
    def safe(q):
        k,f=q
        try:return k,{'status':'CHECKS_RECORDED','data':f()}
        except Exception as e:return k,{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=4) as ex:r=dict(ex.map(safe,tasks))
    r.update(canonical_EAM_closed=0,production='NO_GO')
    (OUT/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    (OUT/'README.md').write_text('DM-19. XEMA: Meteocat / Generalitat de Catalunya, original data terms. TRAM static feeds: Powered by TRAM Barcelona https://www.tram.cat ; conditions https://opendata.tram.cat/assets/pdf/condicions_en.pdf ; unchanged ZIPs, publication date in feed metadata when present. TMB NAP public redistribution via Transitland: Powered by MITRAMS https://www.transportes.gob.es/ ; https://nap.transportes.gob.es/licencia-datos ; not direct NAP acquisition. Population: Ajuntament de Barcelona, Oficina Municipal de Dades, Padró 01/01/2026, factual table extraction. No imputation, no production admission. NO-GO.\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error'),'rows':v.get('data',{}).get('rows')} for k,v in r.items() if isinstance(v,dict)},ensure_ascii=False))
if __name__=='__main__':main()
