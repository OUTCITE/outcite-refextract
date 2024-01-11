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
#from flair.data import Sentence
#from flair.models import SequenceTagger
#from joblib import load
#from .outlier_train import extract_features
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index       = sys.argv[1];

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck          = _configs['recheck_O'];

_chunk_size       = _configs['chunk_size_O'];        # how many batch insert in the context of the elasticsearch bulk api
_scroll_size      = _configs['scroll_size_O'];       # how many input docs to retrieve at a time from the index
_max_extract_time = _configs['max_extract_time_O'];  # minutes
_max_scroll_tries = _configs['max_scroll_tries_O'];  # how often to retry when queries failed

_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',
                'anystyle_references_from_gold_fulltext',
                'anystyle_references_from_gold_refstrings',
                'anystyle_references_from_pdftotext_fulltext',
                'cermine_references_from_cermine_xml',
                'cermine_references_from_grobid_refstrings',
                'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml',
                'exparser_references_from_cermine_layout',
                'merged_references' ];

#tagger = SequenceTagger.load('ner')
#clf = load('outlier_detection.joblib')
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def flag_references(refobjs):
    num_flagged = 0;
    for i in range(len(refobjs)):
        if not refobjs[i]:
            continue;
        refobjs[i]['is_outlier'] = False;
        if is_outlier(refobjs[i]):
            refobjs[i]['is_outlier'] = True;
            num_flagged             += 1;
        else:
            refobjs[i]['is_outlier'] = False;
    return refobjs, num_flagged;


def is_outlier(refobj):
    # the length of the whole reference sting is less than 40 characters
    if 'reference' not in refobj or (not refobj['reference']) or len(refobj['reference']) < 40:
        return True

    if len(refobj['reference']) > 1000:
        return True

    # number of tokens
    if 20 < len(refobj['reference'].split()) < 3:
        return True

    is_title_present = 'title' not in refobj or \
                       (not refobj['title']) or (isinstance(refobj['title'], str) and len(refobj['title']) < 10)

    is_authors_or_editors_present = ('authors' not in refobj or not refobj['authors']) and \
                                    ('editor' not in refobj or not refobj['editor'])

    if not is_title_present and not is_authors_or_editors_present:
        return True

    # signals that it's not a reference
    weaker_signals_count = 0
    if 3 < count_pattern(refobj['reference'], r'(\b(17|18|19)[0-9][0-9][a-z]?|(20(0|1|2)[0-9])[a-z]?\b)(((\sbis\s)|(\sund\s)|(\sto\s)|(\sand\s)|-|(\s-\s)|(--)|(\s--\s))(\b(17|18|19)[0-9][0-9]|(20(0|1|2)[0-9])\b))?') < 1:
        weaker_signals_count += 1
    if count_pattern(refobj['reference'], r'journal') > 1:
        weaker_signals_count += 1
    if count_pattern(refobj['reference'], r'((pages?)|(\bp\.?)|(seiten?)|(\bs\.?)) [0-9]+') > 1:
        weaker_signals_count += 1
    if whitespace_ratio(refobj['reference']) > 0.25:
        weaker_signals_count += 1

    if weaker_signals_count >= 2:
        return True

    return False


def count_pattern(string, pattern):
    matches = re.findall(pattern, string)
    total_count = len(matches)
    return total_count


def whitespace_ratio(string):
    whitespace_count = sum(1 for char in string if char.isspace())
    char_count = len(string)
    ratio = whitespace_count / char_count
    return ratio


def update_references():
    client = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
    for refobj in _refobjs:
        body_     = { '_op_type': 'update', '_index': _index, '_id': None, '_source': { 'doc': { 'processed_outliers_'+refobj: True } } };
        scr_query = { "bool": { "must": { "term": { 'has_'+refobj: True } }, "must_not": { "term": { 'processed_outliers_'+refobj:True } } } } if not _recheck else { "bool": { "must": { "term": { 'has_'+refobj:True } } } };
        page      = client.search(index=_index, scroll=str(int(_max_extract_time * _scroll_size)) + 'm', size=_scroll_size, query=scr_query)
        sid       = page['_scroll_id']
        returned  = len(page['hits']['hits'])
        page_num  = 0
        while returned > 0:
            for doc in page['hits']['hits']:
                print('updating',doc['_id']);
                #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                refobjs                                        = doc['_source'][refobj] if refobj in doc['_source'] else None;
                refobjs, num_flagged                           = flag_references(refobjs) if refobjs else [[],0];
                body                                           = copy(body_);
                body['_id']                                    = doc['_id'];
                body['_source']['doc'][refobj]                 = refobjs;
                body['_source']['doc']['num_'+refobj]          = len(refobjs) - num_flagged; # Update the number of references by subtracting the number of outliers
                body['_source']['doc']['has_outliers_'+refobj] = num_flagged > 0;
                body['_source']['doc']['num_outliers_'+refobj] = num_flagged;
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

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -SCRIPT------------------------------------------------------------------------------------------------------------------------------------------
_client = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)

i = 0
for success, info in bulk(_client, update_references(), chunk_size=_chunk_size):
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
