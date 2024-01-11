#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from os.path import exists
from pathlib import Path
import M_utils as ut
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# NAME OF THE ELASTICSEARCH INDEX TO UPDATE
_index = sys.argv[1];

# WHETHER TO UPDATE DOCUMENTS WITH IDS FOR WHICH THERE IS ALREADY AN ENTRY
_update = True if len(sys.argv) > 2 and sys.argv[2]=='update' else False;

# HTTP PORT WHERE THE PDFS ARE PROVIDED
_httport = sys.argv[3] if len(sys.argv) > 3 else '8000';

# THE STRUCTURE OF THE BODY THAT IS USED IN THE BULK UPDATES
_body = { '_op_type': 'create',
                      '_index':   _index,
                      '_id':      None,
                      '_source':  { 'id': None,
                                    'index_batch_start': round(time.time(),0)},
        };
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# CREATES A LIST OF STRINGS READING LINES FROM FILE
def lines2list(filename):
    IN    = open(filename);
    lines = [line.rstrip() for line in IN.readlines()];
    IN.close();
    return lines;

# CREATES A STRING READING LINES FROM FILE
def file2string(filename):
    IN   = open(filename);
    text = IN.read().rstrip().strip();
    IN.close();
    return text;

# INSERTS IDS READ FROM A FILE INTO A TEMPLATE DICT
def make_docs(filenames,template_file):
    IN       = open(template_file);
    template = json.load(IN);
    IN.close();
    docs = dict();
    IN   = open(filenames);
    for line in IN.readlines():
        filename_        = line.rstrip();
        doc              = copy(template);
        doc['@id']       = filename_;
        docs[doc['@id']] = doc;
    IN.close();
    return docs;

# CREATES AN ENTRY FOR A DOCUMENT WITH ANNOTATED GOLD REFERENCES LOADED FROM FILE
def get_docs_dict(docs,refstrings,refobjects,devs):
    pdfolder = '/GEOCITE/' if _index=='geocite' else '/CIOFFI/' if _index=='cioffi' else '/SSOAR/';
    for _id in docs:
        body                                   = copy(_body);
        body['_id']                            = _id;
        body['_source']                        = docs[_id];
        body['_source']['has_pdf']             = True;
        body['_source']['pdf']                 = 'http://svko-outcite.gesis.intra:'+_httport+pdfolder+_id+'.pdf';
        body['_source']['has_gold_refstrings'] = True;
        body['_source']['gold_refstrings']     = lines2list(refstrings+_id+'.txt');
        body['_source']['has_gold_refobjects'] = True;
        body['_source']['gold_refobjects']     = [json.loads(el) for el in lines2list(refobjects+_id)];
        body['_source']['development']         = True if _id in devs else False;
        yield body;

# REPLACES DICTIONARIES WITH IDS TO OBJECTS BY A LIST OF THE OBJECTS
def clear_lists(d):
    L = [];
    for key in list(d.keys()):
        if isinstance(d[key],dict):
            pointer = d[key];
            if '@id' in d[key]:
                L.append(d[key]);
                del d[key];
            d[key] = clear_lists(pointer);
    if L != []:
        return L;
    return d;

# GETS SSOAR DOCUMENTS FROM FILE THAT FOLLOWS THE CORRECT SYNTAX
def get_docs_file(infile):
    IN   = open(infile,'r');
    docs = json.load(IN); IN.close();
    for _id in docs['skg']['ssoar']:
        body            = copy(_body);
        body['_id']     = _id;
        body['_source'] = clear_lists(docs['skg']['ssoar'][_id]);
        yield body;

# LOADS ARXIV DOCUMENTS FROM FILE IF THEIR ID IS IN ID FILE OR NO ID FILE IS GIVEN OR CREATE EMPTY DOC IF NO DOCFILE GIVEN
def get_docs_arxiv(infile,id_file=None):
    subset = None;
    if id_file:
        IN        = open(id_file);
        arxiv_ids = set([line.rstrip() for line in IN]);
        subset    = set([ut.ortho_paper_id(arxiv_id,False) for arxiv_id in arxiv_ids]); print(len(subset))
        IN.close();
    if infile:
        IN = open(infile,'r');
        for line in IN:
            doc    = json.loads(line);
            new_id = ut.ortho_paper_id(doc['id'],False);
            if (not subset) or new_id in subset:
                body        = copy(_body);
                body['_id'] = doc['id'];
                for key in doc:
                    body['_source'][key] = doc[key];
                body['_source']['id']              = doc['id'];
                body['_source']['alternative_ids'] = [new_id,doc['id']] if doc['id'] != new_id else [doc['id']];
                body['_source']['authors_parsed']  = [arxiv_author(author_parts) for author_parts in body['_source']['authors_parsed']] if 'authors_parsed' in body['_source'] else None;
                yield body;
        IN.close();
    else:
        for arxiv_id in arxiv_ids:
            body                               = copy(_body);
            body['_id']                        = arxiv_id;
            body['_source']['id']              = arxiv_id;
            body['_source']['alternative_ids'] = list(set([arxiv_id,ut.ortho_paper_id(arxiv_id,False)]));
            yield body;

# CREATES A DOCUMENT BASED ON THE PRESENCE OF A PDF IN INFOLDER VS CHECKFOLDER
def get_docs_folder(infolder,checkfolder):
    for filename in os.listdir(infolder):
        if exists(checkfolder+filename):
            print(filename,'already in USERS/')
            continue;
        if filename.endswith('.pdf'):
            body                  = copy(_body);
            body['_id']           = filename[:-4];
            body['_source']['id'] = body['_id'];
            yield body;

# PARSES THE AUTHOR INFORMATION GIVEN IN ARXIV INTO OUR DATA MODEL
def arxiv_author(author_parts):
    surname    = author_parts[0] if len(author_parts) > 0 else None;
    firstnames = [name    for name in author_parts[1].split() if len(name.replace('.',''))>1] if len(author_parts) > 1 else None;
    initials   = [name[0] for name in author_parts[1].split() if len(name)                >0] if len(author_parts) > 1 else None;
    author_str = ' '.join([firstname for firstname in firstnames]+[initial+'.' for initial in initials[len(firstnames):]]+[surname]) if surname else None;
    return {'author_string':author_str,'initials':initials,'firstnames':firstnames,'surnames':surname};
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
client = ES(['http://localhost:9200'],timeout=60);

# DIFFERENT WAYS TO CREATE A DOCUMENT INDEX DEPENDING ON INPUT DATA
if _index == 'arxiv' or _index == 'arxiv_test':
    infile = 'resources/arxiv_meta/kaggle.json';
    idfile = 'resources/arxiv_meta/sampled_ppid_3.txt' if _index == 'arxiv_test' else None;
    i = 0;
    for success, info in bulk(client,get_docs_arxiv(infile,idfile),raise_on_exception=False,raise_on_error=False):
        i += 1;
        if not success:
            print('A document failed:', info['create']['_id'], info['create']['error']);
        elif i % 10000 == 0:
            print(i);
elif _index.startswith('arxiv'): #Some arxiv index, but not a subset of the kaggle metadata dump
    idfile = 'resources/arxiv_meta/wolf_arxiv.txt' #TODO: This would need to be changed manually!
    i = 0;
    for success, info in bulk(client,get_docs_arxiv(None,idfile)):
        i += 1;
        if not success:
            print('A document failed:', info['create']['_id'], info['create']['error']);
        elif i % 10000 == 0:
            print(i);
elif _index == 'users':
    infolder    = str((Path(__file__).parent / '../pdfs/UPLOADS/').resolve())+'/';
    checkfolder = str((Path(__file__).parent / '../pdfs/USERS/'  ).resolve())+'/';
    i           = 0;
    for success, info in bulk(client,get_docs_folder(infolder,checkfolder),raise_on_exception=False,raise_on_error=False):
        i += 1;
        if not success:
            print('A document failed:', info['create']['_id'], info['create']['error']);
        elif i % 10000 == 0:
            print(i);
elif _index == 'ssoar' or _index == 'outcite_ssoar' or _index == 'vadis_ssoar':
    infile = 'resources/ssoar_meta/ssoar_GWS.json';
    i      = 0;
    for success, info in bulk(client,get_docs_file(infile),raise_on_exception=False,raise_on_error=False):
        i += 1;
        if not success:
            print('A document failed:', info['create']['_id'], info['create']['error']);
        elif i % 10000 == 0:
            print(i);
elif _index == 'ssoar_test':
    infile = 'resources/ssoar_meta/ssoar_GWS_test_subset100.json';
    i      = 0;
    for success, info in bulk(client,get_docs_file(infile),raise_on_exception=False,raise_on_error=False):
        i += 1;
        if not success:
            print('A document failed:', info['create']['_id'], info['create']['error']);
        elif i % 10000 == 0:
            print(i);
elif _index == 'ssoar_gold' or _index == 'geocite' or _index == 'cioffi':
    infile      = 'resources/gold_references_'+_index+'/docs.txt';
    template    = 'resources/'+_index+'_empty.json';
    refstrings  = 'resources/gold_references_'+_index+'/refstrings/';
    refobjects  = 'resources/gold_references_'+_index+'/refobjects/';
    development = 'resources/gold_references_'+_index+'/dev.txt';  IN = open(development);
    devs        = set([line.rstrip() for line in IN]);  IN.close();
    i           = 0;
    for success, info in bulk(client,get_docs_dict(make_docs(infile,template),refstrings,refobjects,devs),raise_on_exception=False,raise_on_error=False):
        i += 1;
        if not success:
            print('A document failed:', info['create']['_id'], info['create']['error']);
        elif i % 10000 == 0:
            print(i);
else:
    print('Undefined index. Doing nothing.');
client.indices.refresh(index=_index);

#-------------------------------------------------------------------------------------------------------------------------------------------------
