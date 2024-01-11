#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
import io
from copy import deepcopy as copy
import subprocess, shlex
import urllib.request
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from PyPDF2 import PdfFileWriter, PdfFileReader
import xmltodict
import xml.etree.ElementTree as ET
from lxml import etree
from pathlib import Path
from ExtracTEI import TeiExtractor
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# THE ELASTICSEARCH INDEX WHERE WE WANT TO ADD THE PDF ADDRESS TO
_index = sys.argv[1];

# WHETHER TO USE WOLF'S CODE TO GET FULLTEXT FROM TEI XML
_wolf = True;

# LOADING CONFIGS FROM FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck          = _configs['recheck_fulltext'];

_chunk_size       = _configs['chunk_size_fulltext'];
_scroll_size      = _configs['scroll_size_fulltext'];
_max_extract_time = _configs['max_extract_time_fulltext']; #minutes
_max_scroll_tries = _configs['max_scroll_tries_fulltext'];
_request_timeout  = 60;

# LIST OF IDS TO TEST THE SCRIPT ON
_ids = None;

# THE STRUCTURE OF THE BODY THAT IS USED IN THE BULK UPDATES
_body = { '_op_type': 'update',
          '_index': _index,
          '_id': None,
          '_source': { 'doc':  {
                                'has_fulltext':       True,
                                'processed_fulltext': True,
                                'fulltext':           None
                                }
                     }
        }

# THE QUERY THAT IS USED TO ITERATE OVER THE DOCUMENTS THAT SHOULD BE UPDATED
_scr_query = {'bool':{'must':{'term':{'has_xml': True}}}} if _recheck else {'bool':{'must':[{'term':{'has_xml': True}}],'must_not':[{'term':{'processed_fulltext': True}}]}};
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# TRYING TO GET THE PARAGRAPHS FROM GROBID XML
def get_paragraphs(d):
    content = [];
    if 'TEI' in d and 'text' in d['TEI'] and isinstance(d['TEI']['text'],dict) and 'body' in d['TEI']['text']:
        return walk_and_find(d['TEI']['text']['body'],content,None,None,'p','#text');
    return content;

# TRYING TO WALK THROUGH AN XML TREE AND RETURNING ALL CONTENTS UNDER A GIVEN KEY NAME
def walk_and_find(d,content_,key_,key__,val_,val__):
    content = content_;
    if isinstance(d,dict):
        for key in d:
            content = walk_and_find(d[key],content_,key,key_,val_,val__);
    elif isinstance(d,list):
        for el in d:
            content = walk_and_find(el,content_,key_,key__,val_,val__);
    elif isinstance(d,str) and (key_==val_ or (key__==val_ and key_==val__)):
        content.append(d);
    return content;

# TRYING TO FIND EXTRACTED REFERENCES TO ADD TO THE FULLTEXT
def get_references(d):
    content = [];
    if 'TEI' in d and 'text' in d['TEI'] and isinstance(d['TEI']['text'],dict) and 'back' in d['TEI']['text']:
        return walk_and_find(d['TEI']['text']['back'],content,None,None,'title','#text');
    return content;

# TRYING TO GET TEXT FROM TEI XML IN RUDIMENTARY WAY
def get_text(xml):
    xml  = xml.replace('</',' </');
    text = None;
    try:
        tree = ET.fromstring(xml);
        text = ET.tostring(tree,encoding='unicode',method='text');
    except:
        pass;
    return text;

# GETTING FULLTEXT FROM TEI XML EITHER IN TOBIAS' OR WOLF'S WAY
def extract(xml,WOLF,SENTENCES=False):
    if WOLF:
        try:
            xml_bytes        = io.BytesIO(xml.encode());
            root             = etree.parse(xml_bytes);
            extractor        = TeiExtractor(root, SENTENCES);
            doc, annotations = extractor.extract();
            return True, doc;
        except Exception as e:
            print(e);
            print('ERROR: Failed to extract fulltext from PDF due to exception!');
            return False, None;
    else:
        D       = None;
        success = True;
        try:
            D = xmltodict.parse(xml);
        except:
            print('xmltodict failed for some reason. Skipping...');
            success = False;
        fulltext = '\n'.join(get_paragraphs(D)) + '\n\n' + '\n'.join(get_references(D)) if D != None else None;
        text     = get_text(xml);
        fulltext = None if fulltext==None and text==None else fulltext if text==None else text if fulltext==None else fulltext+'\n'+text;
        fulltext = None if not isinstance(fulltext,str) else fulltext.replace('\n',' ').replace('\t',' ');
        return success, fulltext;

# TRYING TO EXTRACT THE METADATA FROM GROBID XML WHICH IS USEFUL IF NO OTHER METADATA IS PROVIDED
def extract_meta(xml):
    try:
        D = xmltodict.parse(xml);
    except:
        print('xmltodict failed for some reason. Skipping...');
        return None,None,None;
    title, authors, date = ([],[],[]);
    try:
        title = D['TEI']['teiHeader']['fileDesc']['sourceDesc']['biblStruct']['analytic']['title']['#text'];#walk_and_find(D['TEI']['teiHeader']['fileDesc'],title,'title',None,'#text',None); print(title);
    except Exception as e:
        print("Failed to get title:",e);
        title = '';
    try:
        authors = D['TEI']['teiHeader']['fileDesc']['sourceDesc']['biblStruct']['analytic']['author'];#[author['persName']['forename']['#text']+author['persName']['surname'] for author in D['TEI']['teiHeader']['fileDesc']['sourceDesc']['biblStruct']['analytic']['author']];#walk_and_find(D['TEI']['teiHeader']['fileDesc'],authors,'forename','persName','#text','surname'); print(authors);
    except Exception as e:
        print("Failed to get authors:",e);
        authors = [];
    date    = '';#walk_and_find(D['TEI']['teiHeader']['fileDesc'],date,'date',None,'#when',None); print(date);
    print('title:',title);
    print('authors:',authors);
    print('date:',date);
    return [title if len(title)>0 else None, authors if len(authors)>0 else None, date if len(date)>0 else None];

# THE MAIN FUNCTION TO EXTRACT FULLTEXT FROM TEI XML AND BATCH UPDATE THE RESULT INTO THE ELASTICSEARCH INDEX
def get_fulltexts():
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=_index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=_scr_query);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            print('updating',doc['_id']);
            xml                                          = doc['_source']['xml'];
            success, fulltext                            = extract(xml,_wolf) if isinstance(xml,str) else None; #TODO: Check why there is sometimes a list in ['xml'] field!
            title, authors, date                         = extract_meta(xml) if _index=='users' or _index=='arxiv_wolf' else (None,None,None);
            body                                         = copy(_body);
            body['_id']                                  = doc['_id'];
            body['_source']['doc']                       = doc['_source'];
            body['_source']['doc']['fulltext']           = fulltext;
            body['_source']['doc']['has_fulltext']       = True if fulltext else False;
            body['_source']['doc']['processed_fulltext'] = True;#success;
            if title:
                body['_source']['doc']['title'] = title;
            if authors and _index!='arxiv_wolf': # There were some inconsistencies in the grobid output #TODO: Parse them into our format
                body['_source']['doc']['authors'] = authors;
            if date:
                body['_source']['doc']['date'] = date;
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

# THE BULK UPDATING PROCESS
i = 0;
for success, info in bulk(_client,get_fulltexts(),chunk_size=_chunk_size,request_timeout=_request_timeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#'''
#-------------------------------------------------------------------------------------------------------------------------------------------------
