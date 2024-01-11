# -IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import time
import json
import sqlite3
import re
from bs4 import BeautifulSoup
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
import M_utils as ut
from pathlib import Path
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index   = sys.argv[1]
_workers = int(sys.argv[2]) if len(sys.argv)>2 else 1;

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck          = _configs['recheck_G'];

_chunk_size       = _configs['chunk_size_G'];       # how many batch insert in the context of the elasticsearch bulk api
_scroll_size      = _configs['scroll_size_G'];      # how many input docs to retrieve at a time from the index
_max_extract_time = _configs['max_extract_time_G']; # minutes
_max_scroll_tries = _configs['max_scroll_tries_G']; # how often to retry when queries failed
_request_timeout  = _configs['request_timeout'];

_input_indicator    = 'has_xml'
_output_field       = 'grobid_references_from_grobid_xml'
_output_indicator   = 'processed_'+_output_field
_output_min1        = 'has_'      +_output_field
_output_field_count = 'num_'      +_output_field

_body = {'_op_type': 'update',
         '_index': _index,
         '_id': None,
         '_source': {'doc': {
             _output_field:       [],
             _output_indicator:   False,
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

_scr_query = { "bool": { "must": [ { "term": { _input_indicator: True } } ], "must_not": [ { "term": { _output_indicator: True } } ] } } if not _recheck else {'match_all':{} }

URL = re.compile(_configs['regex_url']); #TODO: MOVE TO CONFIGS!
DOI = re.compile(_configs['regex_doi']); #TODO: MOVE TO CONFIGS!

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def extract_grobid_references(xml,cur):
    refobjs = []
    soup    = BeautifulSoup(xml, "xml")
    root    = soup.find('TEI')
    text    = root.findChildren('text', recursive='false') if root else []
    for children in text:
        refs = children.find('back').find('listBibl').find_all('biblStruct')
        if refs:
            for ref in refs:
                titles      = ref.find_all('title')
                authors     = ref.find_all('author')
                date        = ref.find('date')  # type-> published, when-> year
                editor      = ref.find('editor')
                pub_place   = ref.find('pubPlace')
                publisher   = ref.find('publisher')
                meeting     = ref.find('meeting')  # meeting place name
                bibl_scopes = ref.find_all('biblScope')  # char-> from to, unit-> page or volume
                org_name    = ref.find('orgName')
                notes       = ref.find_all('note')  # e.g type-> e.g. Master thesis or any additional info
                to_id       = ref.find('idno')
                to_type     = to_id.get('type') if to_id else None
                citation_id = ref.get('xml:id')
                refobj      = dict()
                if authors:
                    refobj['authors'] = []
                    for author in authors:
                        auth = dict()
                        forenames = author.find_all('forename')
                        if forenames:
                            auth['initials'] = []
                            for forename in forenames:
                                first_name = forename.get('type') == 'first'
                                middle_name = forename.get('type') == 'middle'
                                if first_name or middle_name:
                                    if 'author_string' not in auth:
                                        auth['author_string'] = ''
                                    cleaned_split_fnames = ut.clean_and_split_text(forename.text, 'str')
                                    for each_fname in cleaned_split_fnames:
                                        if len(each_fname) > 1:
                                            if 'firstnames' not in auth:
                                                auth['firstnames'] = []
                                            auth['firstnames'].append(each_fname)
                                        auth['initials'].append(each_fname[0])
                                        auth['author_string'] = ' '.join([auth['author_string'], each_fname]) if auth['author_string'] != '' else each_fname
                        if author.find('surname'):
                            if 'author_string' not in auth:
                                auth['author_string'] = ''
                            auth['surname'] = author.find('surname').text
                        if 'surname' in auth and (auth['surname'] not in auth['author_string']):  # for appending surname at end of string
                            auth['author_string'] = ' '.join([auth['author_string'], auth['surname']]) if auth['author_string'] != '' else auth['surname']
                        refobj['authors'].append(auth)
                if titles:
                    for title in titles:
                        if title.get('type') == 'main' or len(titles) == 1:
                            refobj['title'] = title.text
                        else:
                            refobj['source'] = title.text
                if date and date.get('when'):
                    cleaned_split_date = ut.clean_and_split_text(date.get('when'), 'int')
                    if cleaned_split_date:
                        refobj['year'] = cleaned_split_date[0]
                if pub_place:
                    split_str = pub_place.text.split(': ')
                    if split_str:
                        refobj['place'] = split_str[0]
                        if len(split_str) > 1:
                            if 'publishers' not in refobj:
                                refobj['publishers'] = []
                            publish = dict()
                            publish['publisher_string'] = split_str[1]
                            refobj['publishers'].append(publish)
                elif meeting:
                    if meeting.find('addrLine'):
                        refobj['place'] = meeting.find('addrLine').text
                if publisher:
                    if 'publishers' not in refobj:
                        refobj['publishers'] = []
                    publish = dict()
                    publish['publisher_string'] = publisher.text
                    refobj['publishers'].append(publish)
                if editor:
                    split_str = editor.text.split(': ')
                    if split_str:
                        refobj['editors'] = []
                        edtr = dict()
                        if len(split_str) > 1:
                            refobj['place'] = split_str[0]
                            edtr['editor_string'] = split_str[1]
                            refobj['editors'].append(edtr)
                        else:
                            edtr['editor_string'] = split_str[0]
                            refobj['editors'].append(edtr)
                for bibl_scope in bibl_scopes:
                    if bibl_scope.get('unit') == 'page':
                        if bibl_scope.text:
                            cleaned_split_pages = ut.clean_and_split_text(bibl_scope.text, 'int')
                            if cleaned_split_pages:
                                refobj['start'] = cleaned_split_pages[0]
                                if len(cleaned_split_pages) > 1 and cleaned_split_pages[0] < cleaned_split_pages[1]:  # page start must be smaller than page end
                                    refobj['end'] = cleaned_split_pages[1]
                        else:
                            cleaned_split_start_pg = ut.clean_and_split_text(bibl_scope.get('from'), 'int')
                            cleaned_split_end_pg = ut.clean_and_split_text(bibl_scope.get('to'), 'int')
                            if cleaned_split_start_pg:
                                refobj['start'] = cleaned_split_start_pg[0]
                                if cleaned_split_end_pg and cleaned_split_start_pg[0] < cleaned_split_end_pg[0]:
                                    refobj['end'] = cleaned_split_end_pg[0]
                    elif bibl_scope.get('unit') == 'volume':
                        cleaned_split_vol = ut.clean_and_split_text(bibl_scope.text, 'int')
                        if cleaned_split_vol:
                            refobj['volume'] = cleaned_split_vol[0]
                    elif bibl_scope.get('unit') == 'issue':
                        cleaned_split_issue = ut.clean_and_split_text(bibl_scope.text, 'int')
                        if cleaned_split_issue:
                            refobj['issue'] = cleaned_split_issue[0]
                for note in notes:
                    if note.get('type') == 'raw_reference':
                        refobj['reference'] = note.text
                if to_id and to_type:
                    to_type_         = to_type.lower();
                    to_id_           = to_id.text.lower().split(to_type_+':')[-1].strip();
                    refobj[to_type_] = to_id_
                    print(to_type_,to_id_)
                refobj['inline_id'] = citation_id
                #urls = [match.group(0) for match in URL.finditer(refobj['reference'])] if 'reference' in refobj and refobj['reference'] else [];
                #if urls:
                    #print(refobj['reference'],'\nurl',urls);
                urls = [];
                for url_ in [match.group(0) for match in URL.finditer(refobj['reference'])]:
                    url_s = url_.split(' ');
                    longest_url = None;
                    for i in range(len(url_s)):
                        url         = ''.join(url_s[:i+1]);
                        url         = url[:-1] if url.endswith('.') else url;
                        url         = ut.check(url,False,cur,3);
                        longest_url = url if url else longest_url;
                    urls = urls + [longest_url] if longest_url and not (('doi' in refobj and refobj['doi'] and refobj['doi'].lower() in longest_url.lower()) or ('arxiv' in refobj and refobj['arxiv'] and refobj['arxiv'].lower() in longest_url.lower())) else urls;
                if urls:
                    refobj['url'] = urls[0]; # There are hardly ever multiple URLS in a reference string
                    print('url',urls[0])
                refobjs.append(refobj)
    return refobjs, True #TODO: Any conditions within this function for not success?


def update_grobid_references(IDs,client,worker):
    #con = sqlite3.connect('grobid_urls.db');
    cur = None; #con.cursor();
    #cur.execute("CREATE TABLE IF NOT EXISTS urls(url TEXT PRIMARY KEY, status INTEGER, resolve TEXT)");
    page     = client.search(index=_index, scroll=str(int(_max_extract_time * _scroll_size)) + 'm', size=_scroll_size, query={"ids":{"values":IDs}})
    sid      = page['_scroll_id']
    returned = len(page['hits']['hits'])
    page_num = 0
    while returned > 0:
        for doc in page['hits']['hits']:
            #if _output_min1 in doc['_source'] and doc['_source'][_output_min1]:
            #    body                                      = copy(_body);
            #    body['_id']                               = doc['_id'];
            #    body['_source']['doc'][_output_field]     = doc['_source'][_output_field];
            #    body['_source']['doc'][_output_min1]      = True;
            #    body['_source']['doc'][_output_indicator] = True;
            #    yield body
            #    continue;
            print('updating',doc['_id'])
            # ---------------------------------------------------------------------------------------------------------------------------------------
            xml                                         = doc['_source']['xml'] if '_source' in doc and 'xml' in doc['_source'] else None
            refobjs, success                            = extract_grobid_references(xml,cur) if xml and isinstance(xml, str) else ([],False)
            body                                        = copy(_body)
            body['_id']                                 = doc['_id']
            body['_source']['doc'][_output_field]       = refobjs if success else [];
            body['_source']['doc'][_output_indicator]   = success
            body['_source']['doc'][_output_min1]        = len(refobjs)>0 if success else False
            body['_source']['doc'][_output_field_count] = len(refobjs)   if success else 0
            #con.commit();
            # ---------------------------------------------------------------------------------------------------------------------------------------
            yield body
        scroll_tries = 0
        while scroll_tries < _max_scroll_tries:
            try:
                page = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time * _scroll_size)) + 'm')
                returned = len(page['hits']['hits'])
                page_num += 1
            except Exception as e:
                print(e)
                print('\n[!]-----> Some problem occurred while scrolling. Sleeping for 3s and retrying...')
                returned = 0
                scroll_tries += 1
                time.sleep(3)
                continue
            break
    client.clear_scroll(scroll_id=sid);
    #con.close();

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

ut.process(update_grobid_references, _index, list(ut.make_batches(_index,_scr_query,_max_extract_time,_scroll_size,_max_scroll_tries)), _chunk_size, _request_timeout, _workers)
