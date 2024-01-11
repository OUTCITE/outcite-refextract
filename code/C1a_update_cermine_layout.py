import os, sys
import shutil
import time
import subprocess
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from pathlib import Path
import M_utils as ut
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index = sys.argv[1]

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck = _configs['recheck_C1a'];

_chunk_size       = _configs['chunk_size_C1a'];  # how many batch insert in the context of the elasticsearch bulk api
_scroll_size      = _configs['scroll_size_C1a'];  # how many input docs to retrieve at a time from the index
_max_extract_time = _configs['max_extract_time_C1a'];  # minutes
_max_scroll_tries = _configs['max_scroll_tries_C1a'];  # how often to retry when queries failed
_request_timeout  = _configs['request_timeout'];
_pdf_file         = _configs['pdf_file_C1a'];
_layout_file_csv  = _configs['layout_file_csv_C1a'];
_layout_file_xml  = _configs['layout_file_xml_C1a'];
_layout_file_txt  = _configs['layout_file_txt_C1a'];
_max_mb           = _configs['max_mb_C1a'];
_suffix           = _configs['suffix_C1a'];

_cermine = _configs['cermine_C1a'];

_tmpdir           = str((Path(__file__).parent / '../temp/layout_pdfs/').resolve())+'/';

_input_field      = 'pdf'
_input_indicator  = 'has_'+_input_field
_output_field     = 'cermine_layout'
_output_indicator = 'processed_'+_output_field
_output_min1      = 'has_'      +_output_field

_body = {
    '_op_type': 'update',
    '_index': _index,
    '_id': None,
    '_source': {
            'doc': {
                _output_field:     None,
                _output_indicator: False,
                _output_min1:      False
                    }
                }
         }

_scr_query = { "bool": { "must": [ { "term": { _input_indicator: True } } ], "must_not": [ { "term": { _output_indicator: True } } ] } } if not _recheck else {'match_all':{}}


def update_cermine_xml():
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
    page     = client.search(index=_index, scroll=str(int(_max_extract_time * _scroll_size)) + 'm', size=_scroll_size, query=_scr_query)
    sid      = page['_scroll_id']
    returned = len(page['hits']['hits'])
    page_num = 0
    while returned > 0:
        for doc in page['hits']['hits']:
            print('updating',doc['_id']);
            # ---------------------------------------------------------------------------------------------------------------------------------------
            pdf                                       = doc['_source'][_input_field] if '_source' in doc and _input_field in doc['_source'] else None
            success                                   = ut.download_pdf(pdf, _tmpdir + _pdf_file) if pdf else False
            layouts, success                          = ut.obtain_results(["java", "-jar", _cermine, _tmpdir, _tmpdir, _suffix],[_tmpdir+_layout_file_csv,_tmpdir+_layout_file_xml,_tmpdir+_layout_file_txt]) if success else (None, False)
            body                                      = copy(_body)
            body['_id']                               = doc['_id']
            body['_source']['doc'][_output_field]     = layouts[0] if layouts and layouts[0] else None
            body['_source']['doc'][_output_indicator] = success
            body['_source']['doc'][_output_min1]      = True if layouts and layouts[0] else False;
            # ---------------------------------------------------------------------------------------------------------------------------------------
            yield body
        scroll_tries = 0
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time * _scroll_size)) + 'm')
                returned  = len(page['hits']['hits'])
                page_num += 1
            except Exception as e:
                print(e)
                print('\n[!]-----> Some problem occurred while scrolling. Sleeping for 3s and retrying...\n')
                returned      = 0
                scroll_tries += 1
                time.sleep(3)
                continue
            break
    client.clear_scroll(scroll_id=sid);

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)

i = 0
for success, info in bulk(_client, update_cermine_xml(), chunk_size=_chunk_size,request_timeout=_request_timeout):
    i += 1
    print('######', i, '#######################################################################')
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'], '\n')
    if i % _chunk_size == 0:
        print(i, ' refreshing...')
        _client.indices.refresh(index=_index)
        print(i, ' refreshed...')
_client.indices.refresh(index=_index)
print(i, ' Refreshed and Process Ended...!!!')
