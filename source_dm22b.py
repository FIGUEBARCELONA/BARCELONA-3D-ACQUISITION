"""DM22b: targeted alternate ATM source and public statistical UI read.
No credential submission, no imputation, no FGC reconciliation repetition."""
import collections as C, json,csv,io,zipfile,hashlib,urllib.request,gzip,os,re,shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import independent_dm20c as base
O=Path('custody/dm22b');O.mkdir(parents=True,exist_ok=True)
def sha(b):return hashlib.sha256(b).hexdigest()
def save(name,b):
    p=O/name;p.write_bytes(b);return {'path':str(p),'bytes':len(b),'sha256':sha(b)}
def req(x,m):
    if not x:raise ValueError(m)
def atm():
    u='https://files.mobilitydatabase.org/mdb-2826/mdb-2826-202611170044/mdb-2826-202611170044.zip'
    with urllib.request.urlopen(urllib.request.Request(u,headers={'Accept-Encoding':'identity','User-Agent':'Barcelona3D open data QA'}),timeout=60) as r:
        raw=r.read(100*1024**2+1);req(len(raw)<=100*1024**2 and r.status==200,'archive cap or response')
    old=json.loads(Path('custody/dm20d/report.json').read_text())['ATM_exception_report']['short_trip_details']
    target={x['trip_id']:x for x in old};req(len(target)==39,'target count')
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        req(sum(x.file_size for x in z.infolist())<=700*1024**2,'expanded cap')
        req(z.testzip() is None,'CRC')
        trips={x['trip_id']:x for x in base.rows(z,'trips') if x['trip_id'] in target}
        services={x['service_id'] for x in trips.values()}
        active=set()
        for x in base.rows(z,'calendar'):
            if x['service_id'] in services and base.date(x['start_date'])<=base.DAY<=base.date(x['end_date']) and x[base.DAY.strftime('%A').lower()]=='1':active.add(x['service_id'])
        for x in base.rows(z,'calendar_dates'):
            if x['service_id'] in services and base.date(x['date'])==base.DAY:
                if x['exception_type']=='1':active.add(x['service_id'])
                else:active.discard(x['service_id'])
        groups=C.defaultdict(list)
        for x in base.rows(z,'stop_times'):
            if x['trip_id'] in trips:groups[x['trip_id']].append(x)
        results=[]
        for t,p in target.items():
            rs=groups[t]
            results.append({'trip_id':t,'simplified_operator':p['operator'],'simplified_active':p['active_on_date'],'simplified_stop_rows':p['stop_times'],'full_trip_present':t in trips,'full_active_on_date':trips.get(t,{}).get('service_id') in active,'full_stop_rows':len(rs),'full_timed_rows':sum(bool(x.get('arrival_time')) and bool(x.get('departure_time')) for x in rs)})
        ev=save('atm_full_target_rows.json.gz',gzip.compress(json.dumps({'trips':trips,'stop_times':dict(groups)},ensure_ascii=False,separators=(',',':')).encode(),mtime=0))
        return {'source_url':u,'archive_bytes':len(raw),'archive_sha256':sha(raw),'archive_crc_verified':True,'original_archive_not_persisted':True,'target_extract':ev,'target_rows':results,'exact_id_matches':len(trips),'recovered_two_or_more_active_target_ids':sum(x['simplified_active'] and x['full_active_on_date'] and x['full_stop_rows']>=2 for x in results),'full_snapshot_not_replacement':True,'imputed_times':0}
def population():
    from playwright.sync_api import sync_playwright
    u='https://portaldades.ajuntament.barcelona.cat/ca/estad%C3%ADstiques/yzlntdm2fs'
    report={'url':u,'status':'INSPECTED_NOT_VALIDATED','data_candidates':[],'responses':[]}
    with sync_playwright() as p:
        executable=shutil.which('google-chrome') or shutil.which('chromium')
        req(executable,'no preinstalled browser')
        browser=p.chromium.launch(executable_path=executable,headless=True,args=['--no-sandbox'])
        page=browser.new_page(accept_downloads=False)
        def record(resp):
            try:
                if resp.request.resource_type not in ('xhr','fetch'):return
                if not re.search(r'(?:^|[./])(?:ajuntament\.barcelona\.cat|barcelona\.cat)(?:/|$)',resp.url):return
                typ=resp.headers.get('content-type','')
                if len(report['responses'])>=80:return
                report['responses'].append({'url':resp.url,'status':resp.status,'content_type':typ})
                if 'json' not in typ or resp.status!=200:return
                raw=resp.body()
                if len(raw)>8*1024**2:return
                text=raw.decode('utf-8')
                if '2025' in text and re.search(r'Raval|Barceloneta',text,re.I) and len(report['data_candidates'])<4:
                    json.loads(text)
                    report['data_candidates'].append(save('population_candidate_'+str(len(report['data_candidates']))+'.json.gz',gzip.compress(raw,mtime=0)))
                    report['data_candidates'][-1].update(url=resp.url,raw_sha256=sha(raw))
            except Exception:pass
        page.on('response',record)
        try:
            response=page.goto(u,wait_until='domcontentloaded',timeout=35000)
            report['http']=response.status if response else None
            page.wait_for_timeout(14000)
            report['title']=page.title()
            report['body_excerpt']=page.locator('body').inner_text(timeout=5000)[:12000]
            report['tables']=[{'headers':t.locator('thead').inner_text()[:1000],'rows':t.locator('tbody tr').count()} for t in page.locator('table').all()[:8]]
        except Exception as e:report['browser_error']=str(e)[:1000]
        finally:browser.close()
    return report
def main():
    def safe(q):
        k,f=q
        try:return k,{'status':'CHECKS_RECORDED','data':f()}
        except Exception as e:return k,{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=2) as ex:r=dict(ex.map(safe,[('ATM_full',atm),('population_UI',population)]))
    r.update(production='NO_GO',canonical_EAM_closed=0)
    (O/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error')} for k,v in r.items() if isinstance(v,dict)}))
if __name__=='__main__':main()
