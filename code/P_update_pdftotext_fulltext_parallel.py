import multiprocessing
import os, sys
import shutil
import time
import subprocess
import json
import urllib.request
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from pathlib import Path
import M_utils as ut
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index   = sys.argv[1];
_workers = int(sys.argv[2]) if len(sys.argv)>2 else 1;

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck = _configs['recheck_P'];

_chunk_size       = _configs['chunk_size_P'];  # how many batch insert in the context of the elasticsearch bulk api
_scroll_size      = _configs['scroll_size_P'];  # how many input docs to retrieve at a time from the index
_max_extract_time = _configs['max_extract_time_P'];  # minutes
_max_scroll_tries = _configs['max_scroll_tries_P'];  # how often to retry when queries failed
_request_timeout  = _configs['request_timeout'];

_pdftotext = _configs['pdftotext'];

_tmpdir           = str((Path(__file__).parent / '../temp/').resolve())+'/';

_input_field      = 'pdf'
_input_indicator  = 'has_'+_input_field
_output_field     = 'pdftotext_fulltext'
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

_scr_query = {"bool":{"must": [ {"term":{_input_indicator:True}} ], "must_not": [ {"term":{_output_indicator:True}} ]}} if not _recheck else {'match_all':{}}


def update_pdftotext_fulltext(IDs,client,worker):
    page     = client.search(index=_index, scroll=str(int(_max_extract_time * _scroll_size)) + 'm', size=_scroll_size, query={"ids":{"values":IDs}})
    sid      = page['_scroll_id']
    returned = len(page['hits']['hits'])
    page_num = 0
    while returned > 0:
        for doc in page['hits']['hits']:
            print('updating',doc['_id']);
            # ---------------------------------------------------------------------------------------------------------------------------------------
            pdf                                       = doc['_source'][_input_field] if '_source' in doc and _input_field in doc['_source'] else None
            success                                   = ut.download_pdf(pdf, _tmpdir + 'tmp_pdftotext_fulltext_'+str(worker)+'.pdf') if pdf else False
            fulltexts, success                        = ut.obtain_results([_pdftotext,_tmpdir+'tmp_pdftotext_fulltext_'+str(worker)+'.pdf',_tmpdir+'tmp_pdftotext_fulltext_'+str(worker)+'.txt'],[_tmpdir+'tmp_pdftotext_fulltext_'+str(worker)+'.txt']) if success else (None, False)
            body                                      = copy(_body)
            body['_id']                               = doc['_id']
            body['_source']['doc'][_output_field]     = fulltexts[0] if fulltexts else None
            body['_source']['doc'][_output_indicator] = True;
            body['_source']['doc'][_output_min1]      = success;
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

ut.process(update_pdftotext_fulltext, _index, list(ut.make_batches(_index,_scr_query,_max_extract_time,_scroll_size,_max_scroll_tries)), _chunk_size, _request_timeout, _workers)
