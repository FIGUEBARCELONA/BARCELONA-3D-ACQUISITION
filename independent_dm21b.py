"""DM21b: geometry readback and conservative residual FGC crosswalk."""
import independent_dm21 as c
import independent_dm20d as d
from pathlib import Path
import collections as C,json,gzip,hashlib,unicodedata,re,math
from concurrent.futures import ThreadPoolExecutor
from shapely.geometry import shape
from shapely.ops import unary_union
from shapely.validation import explain_validity
O=Path('custody/dm21b');O.mkdir(parents=True,exist_ok=True)
def req(x,m):
    if not x:raise ValueError(m)
def geometry():
    r=json.loads(Path('custody/dm21/report.json').read_text())['geometry']['data']
    p=Path(r['source']['path']);b=p.read_bytes();raw=gzip.decompress(b)
    req(hashlib.sha256(b).hexdigest()==r['source']['sha256'] and hashlib.sha256(raw).hexdigest()==r['raw_sha256'],'hash')
    j=json.loads(raw);req(j['estat']=='OK','service status');rs=j['resultats']
    pop=json.loads(Path('custody/dm20b/primary-population-reconciliation.json').read_text())['rows']
    by={int(x['codi']):x for x in rs};req(len(rs)==len(by)==73 and set(by)==set(range(1,74)),'codes')
    def nm(s):return ''.join(ch for ch in unicodedata.normalize('NFKD',s).casefold() if ch.isalnum())
    rows=[];polys={}
    for x in pop:
        k=x['code'];g=by[k];gg=g['geometria'];req(gg['proj']=='EPSG:25831','CRS');geom=shape(gg);polys[k]=geom
        rows.append({'code':k,'district_OMD':x['district'],'barri_OMD':x['barri'],'barri_GeoBCN':g['nom'],'name_match_normalized':nm(x['barri'])==nm(g['nom']),'pob_2026':x['population'],'valid':geom.is_valid,'validity':explain_validity(geom),'type':geom.geom_type,'area_m2':geom.area,'bounds':list(geom.bounds)})
    req(len(rows)==73 and sum(x['pob_2026'] for x in rows)==1729963,'pop sum')
    valid=all(x['valid'] for x in rows);u=None;overlap=None;inter=[]
    if valid:
        u=unary_union(list(polys.values()))
        for i,g in polys.items():
            for k,h in polys.items():
                if k>i and g.intersects(h):
                    area=g.intersection(h).area
                    if area>0:inter.append({'codes':[i,k],'area_m2':area})
        overlap=sum(x['area_m2'] for x in rows)-u.area
    # Compare official current district API by same CRS; never repair geometry.
    district={}
    try:
        url='https://geoportal.barcelona.cat/geoBCN/serveis/territori/districtes/?extensio=true&geometria=true&out_proj=EPSG:25831'
        raw2=c.get(url,12*1024**2);j2=json.loads(raw2);req(j2['estat']=='OK','district response')
        p2=O/'geobcn_districtes.json.gz';p2.write_bytes(gzip.compress(raw2,mtime=0));src={'path':str(p2),'sha256':hashlib.sha256(p2.read_bytes()).hexdigest(),'bytes':p2.stat().st_size}
        rs2=j2['resultats'];req(len(rs2)==10,'district count')
        district={'source':src,'raw_sha256':hashlib.sha256(raw2).hexdigest(),'units':[]}
        for x in rs2:
            req(x['geometria']['proj']=='EPSG:25831','district CRS')
            g=shape(x['geometria']);name=x['nom'];match=[r for r in rows if nm(r['district_OMD'])==nm(name)]
            ret={'code':x['codi'],'name':name,'valid':g.is_valid,'matched_barris':len(match),'population_2026':sum(r['pob_2026'] for r in match)}
            if match and valid and g.is_valid:
                agg=unary_union([polys[r['code']] for r in match]);ret.update(symmetric_difference_m2=agg.symmetric_difference(g).area,official_area_m2=g.area)
            district['units'].append(ret)
    except Exception as e:district['error']=str(e)
    return {'rows':rows,'matched_codes':len(rows),'name_matches':sum(x['name_match_normalized'] for x in rows),'valid_geometries':sum(x['valid'] for x in rows),'sum_areas_m2':sum(x['area_m2'] for x in rows),'union_area_m2':u.area if u is not None else None,'sum_minus_union_m2':overlap,'positive_area_intersections':inter,'district':district,'temporal_geometry_version_certified':False}
def residual():
    a=d.audit(d.sources[0][1],d.sources[0][2]);b=d.audit(d.sources[1][1],d.sources[1][2])
    old=json.loads(Path('custody/dm21/report.json').read_text())['FGC']['data']['crosswalk']
    mp={r['direct']:r['atm'] for r in old}
    aa=a['stop_ids_by_op']['FGC']-set(mp.values());bb=b['stop_ids_by_op']['FGC']-set(mp)
    pairs=[];add={}
    for s in sorted(bb):
        x=b['stops'][s];same=[t for t in aa if c.norm(a['stops'][t]['stop_name'])==c.norm(x['stop_name'])]
        pairs.append({'direct':s,'name':x['stop_name'],'candidates':[{'atm':t,'name':a['stops'][t]['stop_name'],'distance_m':c.dist(x,a['stops'][t])} for t in sorted(aa)]})
        if len(same)==1 and sum(c.norm(b['stops'][q]['stop_name'])==c.norm(x['stop_name']) for q in bb)==1:add[s]=same[0]
    mp.update(add);req(len(set(mp.values()))==len(mp),'not bijective')
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
    ap,ae,_=fp(a);bp,be,u=fp(b,mp)
    return {'residual_pairs':pairs,'new_name_unique_candidates':add,'candidate_total':len(mp),'trip_multiset_matches':sum((ap&bp).values()),'edge_matches':len(ae&be),'unmapped_trips':u,'production_merges':0,'physical_identity_approved':False}
def main():
    def safe(x):
        k,f=x
        try:return k,{'status':'CHECKS_RECORDED','data':f()}
        except Exception as e:return k,{'status':'FAILED','error':str(e)}
    with ThreadPoolExecutor(max_workers=2) as ex:r=dict(ex.map(safe,[('geometry',geometry),('FGC',residual)]))
    r['production']='NO_GO';(O/'report.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    print(json.dumps({k:{'status':v.get('status'),'error':v.get('error')} for k,v in r.items() if isinstance(v,dict)}))
if __name__=='__main__':main()
