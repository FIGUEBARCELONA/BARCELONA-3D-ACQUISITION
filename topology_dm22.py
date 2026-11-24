"""DM22: offline topological comparison of original municipal geometries; no repair."""
import json,gzip,hashlib,unicodedata,collections
from pathlib import Path
from shapely.geometry import shape,Polygon,MultiPolygon
from shapely.ops import unary_union
from shapely.validation import explain_validity
import shapely
O=Path('custody/dm22');O.mkdir(parents=True,exist_ok=True)
def sha(b):return hashlib.sha256(b).hexdigest()
def req(b,m):
    if not b:raise ValueError(m)
def read(path,h,compressed=False):
    b=Path(path).read_bytes()
    if compressed:b=gzip.decompress(b)
    req(sha(b)==h,'source SHA256 '+path)
    return json.loads(b)
def norm(s):return ''.join(ch for ch in unicodedata.normalize('NFKD',s).casefold() if ch.isalnum())
def polygon(g):
    req(g['proj']=='EPSG:25831','CRS')
    p=shape(g);req(p.geom_type in ('Polygon','MultiPolygon') and p.is_valid and not p.is_empty,explain_validity(p));return p
def metrics(a,b):
    return {'area_a_m2':a.area,'area_b_m2':b.area,'a_only_m2':a.difference(b).area,'b_only_m2':b.difference(a).area,'symmetric_difference_m2':a.symmetric_difference(b).area,'intersection_m2':a.intersection(b).area,'hausdorff_discrete_m':a.hausdorff_distance(b)}
def rings(g):
    return list(g.geoms) if g.geom_type=='MultiPolygon' else [g]
def esri_geometry(obj):
    # Native ArcGIS polygon: clockwise exteriors and CCW holes. Assign holes only to unique smallest containing exterior.
    shells=[];holes=[]
    for r in obj['rings']:
        p=Polygon(r)
        signed=sum(a[0]*b[1]-b[0]*a[1] for a,b in zip(r,r[1:]))/2
        req(p.is_valid and signed!=0,'invalid ring')
        (shells if signed<0 else holes).append(p)
    owned=collections.defaultdict(list)
    for h in holes:
        k=[i for i,s in enumerate(shells) if s.covers(h)]
        req(k,'unowned hole')
        i=min(k,key=lambda i:shells[i].area);owned[i].append(h.exterior.coords)
    p=MultiPolygon([Polygon(s.exterior.coords,owned[i]) for i,s in enumerate(shells)])
    req(p.is_valid,explain_validity(p));return p
def topology():
    b=read('custody/dm21/geobcn_barris.json.gz','6bacf44fa2076620b519d0dfd6de33dddc35d77fd4390cda3919593aff9dd0db',True)['resultats']
    d=read('custody/dm21b/geobcn_districtes.json.gz','50ca6a8d4efa0b5b5ad669a1efbbea52ddc9df4503203a6ee3301f229f252f6e',True)['resultats']
    p=read('custody/dm11/ICGC_080193.json','c5605416b9e25b84eaaa6f9eb7a2bb8e15622644de2a40a799dcd1911be1078e')
    pop=json.loads(Path('custody/dm20b/primary-population-reconciliation.json').read_text())['rows']
    B={int(x['codi']):polygon(x['geometria']) for x in b}
    req(len(B)==73,'barris')
    req(len(d)==10,'districts')
    names={norm(x['district']) for x in pop}
    # Discover actual field only by complete one-to-one match of normalized district names.
    fields=[k for k in d[0] if all(isinstance(x.get(k),str) for x in d) and {norm(x[k]) for x in d}==names]
    if len(fields)!=1:
        return {'status':'SCHEMA_UNRESOLVED','keys':list(d[0]),'district_scalars':[{k:v for k,v in x.items() if isinstance(v,(str,int,float))} for x in d],'candidate_fields':fields}
    field=fields[0]
    D={norm(x[field]):polygon(x['geometria']) for x in d};req(len(D)==10,'district identity')
    U=unary_union(list(B.values()));V=unary_union(list(D.values()))
    req(len(p['features'])==1 and p['spatialReference'].get('wkid')==25831,'ICGC CRS / feature')
    M=esri_geometry(p['features'][0]['geometry'])
    req(abs(M.area-101822466.31984997)<0.01,'prior ICGC area')
    comparisons=[]
    for x in d:
        n=norm(x[field]);ids=[r['code'] for r in pop if norm(r['district'])==n]
        A=unary_union([B[i] for i in ids]);comparisons.append({'name':x[field],'code':x.get('codi'),'barris':ids,**metrics(A,D[n])})
    req(sum(len(x['barris']) for x in comparisons)==73,'assignment')
    pairs=[]
    for i,(k,g) in enumerate(D.items()):
        for l,h in list(D.items())[i+1:]:
            v=g.intersection(h).area
            if v>0:pairs.append({'districts':[k,l],'area_m2':v})
    def describe(u):return {'parts':len(rings(u)),'interior_rings':sum(len(x.interiors) for x in rings(u)),'interior_ring_area_m2':sum(Polygon(r).area for x in rings(u) for r in x.interiors)}
    return {'status':'CHECKS_RECORDED','district_name_field':field,'district_valid':len(D),'barris_valid':len(B),'district_comparisons':comparisons,'district_overlaps':pairs,'district_sum_minus_union_m2':sum(g.area for g in D.values())-V.area,'barris_union_vs_district_union':metrics(U,V),'district_union_vs_ICGC':metrics(V,M),'barris_union_vs_ICGC':metrics(U,M),'barris_union':describe(U),'district_union':describe(V),'municipality':describe(M),'geos':shapely.geos_version_string,'originals_modified':False,'tolerance_approved':False,'same_boundary_version_verified':False}
def main():
    try:r=topology()
    except Exception as e:r={'status':'FAILED','error':str(e)}
    r.update(production='NO_GO',canonical_EAM_closed=0)
    (O/'topology.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    print(json.dumps({k:v for k,v in r.items() if k not in ('district_comparisons','district_overlaps')},ensure_ascii=False))
if __name__=='__main__':main()
