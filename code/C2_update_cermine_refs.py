#-IMPORTS-------------------------------------------------------------------------------------------------------------------------------------------
from datetime import datetime
import sys
import multiprocessing as mp
import random
import time
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES, helpers
from elasticsearch.helpers import streaming_bulk as bulk
import bibtexparser
import requests
import json
import M_utils as ut
import re
from pathlib import Path
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBALS-------------------------------------------------------------------------------------------------------------------------------------------

_index   = sys.argv[1];
_source  = sys.argv[2];
_workers = int(sys.argv[3]) if len(sys.argv)>3 else 1;

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck          = _configs['recheck_C2'];

_max_extract_time = _configs['max_extract_time_C2'];    # minutes
_max_scroll_tries = _configs['max_scroll_tries_C2'];    # how often to retry when queries failed
_scroll_size      = _configs['scroll_size_C2'];   # how many input docs to retrieve at a time from the index
_chunk_size       = _configs['chunk_size_C2'];    # how many batch insert in the context of the elasticsearch bulk api
_request_timeout  = _configs['request_timeout'];

_cermine = _configs['cermine_C2'];

_input_indicator    = "has_xml" if _source=='grobid' else "has_cermine_xml" if _source=='cermine' else "has_gold_refstrings" if _source=='gold' else None;
_output_field       = 'cermine_references_from_'+_source+'_refstrings' if _source!='cermine' else 'cermine_references_from_cermine_xml';
_output_indicator   = 'processed_'+_output_field;
_output_min1        = 'has_'      +_output_field;
_output_field_count = 'num_'      +_output_field;

_body = {
    '_op_type': 'update',
    '_index': _index,
    '_id': None,
    '_source': {
            'doc': {
                _output_field:       [],
                _output_indicator:   False,
                _output_min1:        False,
                _output_field_count: None,
                'has_sowiport_ids' : False,
                'has_sowiport_urls': False,
                'has_crossref_ids':  False,
                'has_crossref_urls': False,
                'has_dnb_ids':       False,
                'has_dnb_urls':      False,
                'has_dnb_ids':       False,
                'has_dnb_urls':      False
                    }
                }
         }

_scr_query = { "bool": { "must": { "term": { _input_indicator: True } }, "must_not": { "term": { _output_indicator: True } } } } if not _recheck else { "match_all": {} };

# ---------------------------------------------------------------------------------------------------------------------------------------------------
# -FUNCTIONS-----------------------------------------------------------------------------------------------------------------------------------------

def cermine_parse(refstrs,citation_ids):
    print('parsing refstrs by cermine')
    bibtex = '';
    for refstr in refstrs:
        parsed_refs, success = ut.obtain_results(['java', '-cp', _cermine, 'pl.edu.icm.cermine.bibref.CRFBibReferenceParser', '-reference', refstr])
        if not success:
            continue
        bibtex += parsed_refs[0]+'\n';
    refobjs    = ut.parse_bibtex(bibtex);
    for refobj in refobjs:
        print(refobj);
    references = ut.cermine_map(refobjs,refstrs);
    if citation_ids and len(references) == len(citation_ids):
        for i in range(len(references)):
            references[i]['inline_id'] = citation_ids[i];
    return references, True;

def compute(IDs,client,worker):
    print('Processing documents: ', IDs)
    page     = client.search(index=_index, scroll=str(int(_max_extract_time * _scroll_size)) + 'm', size=_scroll_size, query={"ids":{"values":IDs}})
    sid      = page['_scroll_id']
    returned = len(page['hits']['hits'])
    page_num = 0
    while returned > 0:
        for doc in page['hits']['hits']:
            xml                                         = doc['_source']['xml'] if _source=='grobid' and '_source' in doc and 'xml' in doc['_source'] else doc['_source']['cermine_xml'] if _source=='cermine' and '_source' in doc and 'cermine_xml' in doc['_source'] else None
            if _source != 'cermine':
                refstrs, citation_ids = [[gold_refobj['reference'] for gold_refobj in gold_refobjs],None] if _source=='gold' else [[crossref_refobj['reference'] for crossref_refobj in crossref_refobjs if 'reference' in crossref_refobj],None] if _source=='crossref' else ut.xml2refstrs(xml,_source) if isinstance(xml, str) else [None,None]
                #refstrs               = doc['_source']['gold_refstrings'] if _source=='gold' else ut.xml2refstrs(xml,_source) if xml and isinstance(xml, str) else [];
                refobjs, success      = cermine_parse(refstrs,citation_ids) if refstrs!=None else (None,False)
            else:
                refobjs, success = ut.cermine_xml_to_refobjs(xml) if xml!=None else (None,False)
            body                                        = copy(_body)
            body['_id']                                 = doc['_id']  # use same id in modified document to update old one
            body['_source']['doc'][_output_field]       = refobjs  # if cermine_parsed_refs else None  # extend old content
            body['_source']['doc'][_output_indicator]   = success  # if cermine_parsed_refs else False  # however you determine that there was a result
            body['_source']['doc'][_output_min1]        = len(refobjs)>0 if success else False  # if cermine_parsed_refs else False  # however you determine that there was a result
            body['_source']['doc'][_output_field_count] = len(refobjs)   if success else 0
            print(body)
            yield body
        scroll_tries = 0
        while scroll_tries < _max_scroll_tries:
            try:
                page = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time * _scroll_size))+'m')
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

# -SCRIPT---------------------------------------------------------------------------------------------------------------

ut.process(compute, _index, list(ut.make_batches(_index,_scr_query,_max_extract_time,_scroll_size,_max_scroll_tries)), _chunk_size, _request_timeout, _workers)
# ----------------------------------------------------------------------------------------------------------------------
