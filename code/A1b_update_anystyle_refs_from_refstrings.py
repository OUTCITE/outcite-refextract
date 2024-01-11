# -IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import os
import re
import shutil
import subprocess
import sys
import time
import json
from bs4 import BeautifulSoup
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from pathlib import Path
import M_utils as ut
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index   = sys.argv[1];
_source  = sys.argv[2];
_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 1;

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck = _configs['recheck_A1b'];

_chunk_size       = _configs['chunk_size_A1b'];  # how many batch insert in the context of the elasticsearch bulk api
_scroll_size      = _configs['scroll_size_A1b']; # how many input docs to retrieve at a time from the index
_max_extract_time = _configs['max_extract_time_A1b'];  # minutes
_max_scroll_tries = _configs['max_scroll_tries_A1b'];  # how often to retry when queries failed
_request_timeout  = _configs['request_timeout'];

_tmp_dir            = str((Path(__file__).parent / '../temp/').resolve())+'/';

_input_field        = _source+'_xml';
_output_field       = 'anystyle_references_from_'+_source+'_refstrings';
_output_indicator   = 'processed_'+_output_field;
_output_min1        = 'has_'      +_output_field;
_output_field_count = 'num_'      +_output_field;

_body = {'_op_type': 'update',
         '_index': _index,
         '_id': None,
         '_source': { 'doc': { _output_field:       [],
                               _output_indicator:   False,
                               _output_min1:        False,
                               _output_field_count: None,
                               'has_sowiport_ids' : False,
                               'has_sowiport_urls': False,
                               'has_crossref_ids':  False,
                               'has_crossref_urls': False,
                               'has_dnb_ids':       False,
                               'has_dnb_urls':      False,
                               'has_openalex_ids':  False,
                               'has_openalex_urls': False } }
         };  # this is the body for storing the results in the index via updating of the respective entries

_scr_query = { "bool": { "must":     { "term": { "has_"+[_source+'_',''][_source=='grobid']+"xml":True } if _source!='gold' and _source!='crossref' else { "has_gold_refobjects":True } if _source=='gold' else { "has_crossref_references_by_matching":True } },
                         "must_not": { "term": { "processed_anystyle_references_from_"+_source+"_refstrings": True } } } } if not _recheck else { 'match_all':{} };
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def anystyle_parse(refstrs,citation_ids,tmpfile):
    reffile     = tmpfile+'.ref';
    resultfiles = [tmpfile+'.json'];
    with open(reffile, 'w') as f:
        for refstr in refstrs:
            f.write((refstr + '\n'))
    results, success = ut.obtain_results(["anystyle", "-f", 'json', "--overwrite", "parse", reffile, _tmp_dir],resultfiles)  # add parameter shell=True to run on windows
    if not success or len(results) != len(resultfiles):
        return [],False;
    refobjs, refstrings = json.loads(results[0]), refstrs;
    references          = ut.anystyle_map(refobjs,refstrings);
    if citation_ids and len(references) == len(citation_ids):
        for i in range(len(references)):
            references[i]['inline_id'] = citation_ids[i];
    return references, True;

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
            xml_field                                   = _input_field if _source!='grobid' else 'xml';
            xml                                         = doc['_source'][xml_field] if '_source' in doc and xml_field in doc['_source'] else None
            gold_refobjs                                = doc['_source']['gold_refobjects'] if '_source' in doc and 'gold_refobjects' in doc['_source'] else None
            crossref_refobjs                            = doc['_source']['crossref_references_by_matching'] if '_source' in doc and 'crossref_references_by_matching' in doc['_source'] else None
            refstrs, citation_ids                       = [[gold_refobj['reference'] for gold_refobj in gold_refobjs],None] if _source=='gold' else [[crossref_refobj['reference'] for crossref_refobj in crossref_refobjs if 'reference' in crossref_refobj],None] if _source=='crossref' else ut.xml2refstrs(xml,_source) if isinstance(xml, str) else [None,None]
            refobjs, success                            = anystyle_parse(refstrs,citation_ids,_tmp_dir+'tmp_anystyle_from_refstr_'+str(worker)) if refstrs!=None else ([], False)
            body                                        = copy(_body)
            body['_id']                                 = doc['_id']
            body['_source']['doc'][_output_field]       = refobjs
            body['_source']['doc'][_output_indicator]   = success
            body['_source']['doc'][_output_min1]        = len(refobjs)>0 if success else False
            body['_source']['doc'][_output_field_count] = len(refobjs)   if success else 0
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
    #con.close();

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

ut.process(update_anystyle_references, _index, list(ut.make_batches(_index,_scr_query,_max_extract_time,_scroll_size,_max_scroll_tries)), _chunk_size, _request_timeout, _workers);
