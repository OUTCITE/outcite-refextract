# -IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import json
import os
import re
import shutil
import subprocess
import sys
import time
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from pathlib import Path
import M_utils as ut
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# THE ELASTICSEARCH INDEX WHERE WE WANT TO ADD THE PDF ADDRESS TO
_index = sys.argv[1];

# WHICH FULLTEXT FIELD TO USE FOR EXTRACTION OF REFERENCES
_source = sys.argv[2];

# THE NUMBER OF WORKERS TO BE USED FOR SEPARATE CALLS TO ANYSTYLE
_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 1;

# LOADING CONFIGS FROM FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck = _configs['recheck_A1a'];

_chunk_size       = _configs['chunk_size_A1a'];  # how many batch insert in the context of the elasticsearch bulk api
_scroll_size      = _configs['scroll_size_A1a'];  # how many input docs to retrieve at a time from the index
_max_extract_time = _configs['max_extract_time_A1a'];  # minutes
_max_scroll_tries = _configs['max_scroll_tries_A1a'];  # how often to retry when queries failed
_request_timeout  = _configs['request_timeout'];

_tmp_dir      = str((Path(__file__).parent / '../temp/').resolve())+'/';
_tmp_in       = _tmp_dir+'tmp_anystyle_from_fulltext.txt'
_tmp_out_json = _tmp_dir+'tmp_anystyle_from_fulltext.json'
_tmp_out_ref  = _tmp_dir+'tmp_anystyle_from_fulltext.ref'

_input_field          =                                 _source+'_fulltext' if _source!='grobid' else 'fulltext'
_output_field         = 'anystyle_references_from_'    +_source+'_fulltext'
_output_field_2       = 'anystyle_refstrings_from_'    +_source+'_fulltext'
_output_indicator     = 'processed_'+_output_field
_output_indicator_2   = 'processed_'+_output_field_2
_output_min1          = 'has_'+_output_field
_output_min1_2        = 'has_'+_output_field_2
_output_field_count   = 'num_'+_output_field
_output_field_count_2 = 'num_'+_output_field_2

# THE STRUCTURE OF THE BODY THAT IS USED IN THE BULK UPDATES
_body = {
    '_op_type': 'update',
    '_index': _index,
    '_id': None,
    '_source': {
            'doc': {
                _output_field:         [],
                _output_indicator:     False,
                _output_indicator_2:   False,
                _output_min1:          False,
                _output_min1_2:        False,
                _output_field_count:   None,
                _output_field_count_2: None,
                'has_sowiport_ids' :   False,
                'has_sowiport_urls':   False,
                'has_crossref_ids':    False,
                'has_crossref_urls':   False,
                'has_dnb_ids':         False,
                'has_dnb_urls':        False,
                'has_dnb_ids':         False,
                'has_dnb_urls':        False
                    }
                }
    }  # this is the body for storing the results in the index via updating of the respective entries

# THE QUERY THAT IS USED TO ITERATE OVER THE DOCUMENTS THAT SHOULD BE UPDATED
_scr_query = { "bool": { "must":     { "term": { "has_"+_source+"_fulltext":True } if _source!='grobid' else { "has_fulltext":True } },
                                   "must_not": { "term": { _output_indicator: True } } } }  if not _recheck else { 'match_all':{} };
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# PASSES FULLTEXT TO ANYSTYLE TO EXTRACT AND PARSE REFERENCES FROM
def anystyle_extract(fulltext,tmpfile,cur):
    textfile    = tmpfile+'.txt';
    resultfiles = [tmpfile+'.json',tmpfile+'.ref'];
    with open(textfile, 'wb') as f:
        f.write(fulltext.encode('utf-8'))
    results, success = ut.obtain_results(["anystyle", "-f", 'ref,json', "--overwrite", "find", textfile, _tmp_dir],resultfiles)  # add parameter shell=True to run on windows
    if not success:
        return [],[],False;
    if len(results) != len(resultfiles):
        return [],[],True; # Because this will probably be the result of no file created due to no references found. Would be better if the program created an empty file instead.
    refobj_lines, refstr_lines = results;
    refobjs, refstrings        = json.loads(refobj_lines), refstr_lines.split('\n');
    references                 = ut.anystyle_map(refobjs,refstrings,cur);
    return references, refstrings, True;

# THE MAIN FUNCTION TO EXTRACT AND PARSE REFERENCES FROM FULLTEXT USING ANYSTYLE AND BATCH UPDATE THE RESULT INTO THE ELASTICSEARCH INDEX
def update_anystyle_references(IDs,client,worker):
    print('Processing documents: ', IDs)
    #con = sqlite3.connect('extracted_urls_'+str(worker)+'.db'); # TODO: This is quite a mess, we need to duplicate the database in the beginning and then merge it afterwards
    #cur = con.cursor();
    #cur.execute("CREATE TABLE IF NOT EXISTS urls(url TEXT PRIMARY KEY, status INTEGER, resolve TEXT)");
    cur      = None;
    page     = client.search(index=_index, scroll=str(int(_max_extract_time * _scroll_size)) + 'm', size=_scroll_size, query={"ids":{"values":IDs}})
    sid      = page['_scroll_id']
    returned = len(page['hits']['hits'])
    page_num = 0
    while returned > 0:
        for doc in page['hits']['hits']:
            print('updating',doc['_id']);
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            fulltext                                      = doc['_source'][_input_field] if '_source' in doc and _input_field in doc['_source'] else None
            refobjs, refstrs, success                     = anystyle_extract(fulltext,_tmp_dir+'tmp_anystyle_from_fulltext_'+str(worker),cur) if fulltext else ([],[],False)
            body                                          = copy(_body)
            body['_id']                                   = doc['_id']  # use same id in modified document to update old one
            body['_source']['doc'][_output_field]         = refobjs  # if extracted_anystyle_refs else []  # extend old content
            body['_source']['doc'][_output_field_2]       = refstrs  # if extracted_anystyle_refs else []  # extend old content
            body['_source']['doc'][_output_indicator]     = success
            body['_source']['doc'][_output_indicator_2]   = success
            body['_source']['doc'][_output_min1]          = len(refobjs)>0 if success else False
            body['_source']['doc'][_output_min1_2]        = len(refstrs)>0 if success else False
            body['_source']['doc'][_output_field_count]   = len(refobjs) if success else 0
            body['_source']['doc'][_output_field_count_2] = len(refstrs) if success else 0
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            print(body)
            yield body
        scroll_tries = 0
        while scroll_tries < _max_scroll_tries:
            try:
                page = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time * _scroll_size)) + 'm')
                returned = len(page['hits']['hits'])
                page_num += 1
            except Exception as e:
                print(e)
                print('\n[!]-----> Some problem occurred while scrolling. Sleeping for 3s and retrying...\n')
                returned = 0
                scroll_tries += 1
                time.sleep(3)
                continue
            break
    client.clear_scroll(scroll_id=sid);
    #con.close();

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# THE BULK UPDATING PROCESS
ut.process(update_anystyle_references, _index, list(ut.make_batches(_index,_scr_query,_max_extract_time,_scroll_size,_max_scroll_tries)), _chunk_size, _request_timeout, _workers);

