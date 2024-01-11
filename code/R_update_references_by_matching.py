# -IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import json
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from pathlib import Path
import M_utils as ut
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

_index  = sys.argv[1];
_target = sys.argv[2];

_index_match = _target;
_ref_field   = 'referenced_works' if _target=='openalex' else 'reference' if _target=='crossref' else None;

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck = _configs['recheck_R_'+_index_match];

_chunk_size       = _configs['chunk_size_R_'+_index_match];  # how many batch insert in the context of the elasticsearch bulk api
_scroll_size      = _configs['scroll_size_R_'+_index_match]; # how many input docs to retrieve at a time from the index
_max_extract_time = _configs['max_extract_time_R_'+_index_match];  # minutes
_max_scroll_tries = _configs['max_scroll_tries_R_'+_index_match];  # how often to retry when queries failed
_max_rel_diff     = _configs['max_rel_diff_R_'+_index_match];

_request_timeout  = _configs['request_timeout'];

_output_field       = _index_match+'_references_by_matching';
_output_indicator   = 'processed_'+_output_field;
_output_min1        = 'has_'      +_output_field;
_output_field_count = 'num_'      +_output_field;

_body = {'_op_type': 'update',
         '_index': _index,
         '_id': None,
         '_source': { 'doc': { _output_field:       [],
                               _output_indicator:   False,
                               _output_min1:        False,
                               _output_field_count: None } }
         };  # this is the body for storing the results in the index via updating of the respective entries

_scr_query = { "bool": { "must_not": [{ "term": { _output_indicator: True } }] } } if not _recheck else { 'match_all':{} };

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def norm_reference(reference):
    if _target=='openalex':
        refobj = {'openalex_id':reference};#
    else:
        refobj = dict();
        for to,fro in [('reference','unstructured'),('crossref_id','DOI'),('title','article-title'),('startpage','first-page'),('year','year')]:
            if fro in reference:
                refobj[to] = reference[fro];
        if 'author' in reference and reference['author']:
            refobj['authors'] = [{'author_string':reference['author']}];
        if 'journal-title' in reference and reference['journal-title']:
            refobj['containers'] = [{'container_type':'journal', 'container_string':reference['journal-title']}];
            if 'volume' in reference and reference['volume']:
                refobj['containers'][0]['container_volume'] = reference['volume'];
            if 'issue' in reference and reference['issue']:
                refobj['containers'][0]['container_issue'] = reference['issue'];
    return refobj;

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -SCRIPT------------------------------------------------------------------------------------------------------------------------------------------
_client = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)

#_client.indices.put_settings(index=_index, body={"index.mapping.total_fields.limit": 5000})

i = 0
for success, info in bulk(_client, ut.update_matched_references(_index,_index_match,_scr_query,_body,_max_extract_time,_scroll_size,_max_scroll_tries,_max_rel_diff,_ref_field,norm_reference,_output_field), chunk_size=_chunk_size,request_timeout=_request_timeout):
    i += 1
    print('######', i, '#######################################################################')
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'], '\n')
    if i % _chunk_size == 0:
        print(i, 'refreshing...')
        _client.indices.refresh(index=_index)
        print(i, 'refreshed...!!!')
_client.indices.refresh(index=_index)
print(i, ' Refreshed and Process Ended...!!!')
# -------------------------------------------------------------------------------------------------------------------------------------------------
