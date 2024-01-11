#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
import subprocess, shlex
import urllib.request
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from PyPDF2 import PdfFileWriter, PdfFileReader
import xmltodict
import xml.etree.ElementTree as ET
from pathlib import Path
import M_utils as ut
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# THE ELASTICSEARCH INDEX WHERE WE WANT TO ADD THE PDF ADDRESS TO
_index = sys.argv[1];

# HTTP PORT WHERE THE PDFS ARE PROVIDED
_httport = sys.argv[2] if len(sys.argv) > 2 else '8000';

# THE NUMBER OF WORKERS TO BE USED TO SEND QUERIES TO GROBID SERVICE
_workers = int(sys.argv[3]) if len(sys.argv) > 3 else      1;

# LOADING CONFIGS FROM FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck          = _configs['recheck_xml'];

_tmpdir           = str((Path(__file__).parent / '../temp/').resolve())+'/';#sys.argv[2:];
_folder           = 'SSOAR' if _index.endswith('ssoar') or _index.startswith('ssoar') else 'ARXIV' if _index.startswith('arxiv') else _index.upper();
_addr             = 'http://localhost:'+_httport+'/'+_folder+'/#######.pdf';#sys.argv[3];
_pdfname          = 'tmp_grobid_xml';

_grobid           = _configs['grobid_xml'];#'external/grobid-client-python-master/grobid-client.py';#'external/grobid-0.6.0/grobid-core/build/libs/grobid-core-0.6.0-onejar.jar';
_mode             = _configs['mode_xml'];#'processFullText';
_chunk_size       = _configs['chunk_size_xml'];
_scroll_size      = _configs['scroll_size_xml'];
_max_extract_time = _configs['max_extract_time_xml']; #minutes
_max_scroll_tries = _configs['max_scroll_tries_xml'];
_max_mb           = _configs['max_mb_xml'];
_request_timeout  = 60;

# THE STRUCTURE OF THE BODY THAT IS USED IN THE BULK UPDATES
_body = { '_op_type': 'update',
          '_index': _index,
          '_id': None,
          '_source': {'doc': { 'has_xml':       False,
                               'processed_xml': False,
                               'xml':           None
        }}}

# THE QUERY THAT IS USED TO ITERATE OVER THE DOCUMENTS THAT SHOULD BE UPDATED
_scr_query = {'term':{'has_pdf': True}} if _recheck else {'bool':{'must':[{'term':{'has_pdf': True}}],'must_not':[{'term':{'processed_xml': True}}]}};
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# TEMPORARILY DOWNLOADS A PDF FROM A GIVEN ADDRESS SO IT CAN BE SENT TO GROBID SERVICE
def download(address,filename):
    print('\n[-]-----> Trying to download',address,'to',filename,end='\n');
    success = True;
    try:
        pdf_file, headers = urllib.request.urlretrieve(address,filename)
        PDF               = open(pdf_file); PDF.close();
    except Exception as e:
        print(e);
        print('\n[!]-----> Failed to download',address,'to',filename,'\n');
        success = False;
    if success:
        print('\n[o]-----> Successfully downloaded',address,'to',filename,'\n');
    return success;

# SEND REQUEST FOR XML CREATION TO GROBID SERVICE AND PASS ON THE CONTENT OF THE RESULT FILE AS XML
def extract(tmpfile):
    pdffile = tmpfile+'.pdf';
    xmlfile = tmpfile+'.xml';
    call    = "curl -v --form input=@"+pdffile+" --form includeRawCitations=1 --form consolidateCitations=0 "+_grobid; #"curl -v --form input=@./"+tmpdir+"tmp_grobid_xml.pdf "+_grobid+" > "+tmpdir+"tmp_grobid_xml.xml";#"python "+_grobid+" --input "+tmpdir+" --output "+tmpdir+" --config "+_grobhome+" --n 1 --force "+_mode;#"java -Xmx4G -jar "+_grobid+" -gH "+_grobhome+" -dIn "+_tmpdir+" -dOut "+_tmpdir+" -exe "+_mode;
    success = True;#os.system(call) == 0;
    if os.stat(pdffile).st_size > _max_mb*1000000:
        print('\n[!]-----> PDF larger than '+str(_max_mb)+' MB -- skipping.\n')
        return None;
    try:
        OUT = open(xmlfile,'w');
        subprocess.run(shlex.split(call),stdout=OUT,timeout=_max_extract_time*60);
        OUT.close();
    except subprocess.TimeoutExpired:
        print('\n[!]-----> Extraction from',pdffile,'ran too long -- skipping.\n');
        success = False;
    if not success:
        return None;
    print('\n[o]-----> Successfully ran',call,'\n');
    xml, D  = None,None;
    try:
        IN  = open(xmlfile);
        xml = IN.read(); IN.close();
        D   = xmltodict.parse(xml);
    except:
        print('\n[!]-----> Problem reading from',xmlfile,'\n');
    return xml;

# THE MAIN FUNCTION TO SEND A PDF TO GROBID FOR XML EXTRACTION AND BATCH UPDATE THE RESULT INTO THE ELASTICSEARCH INDEX
def compute(IDs,client,worker):
    print('Processing documents: ', IDs)
    HANDLE   = _index.startswith('ssoar') or _index.endswith('ssoar');
    ARXIV    = _index.startswith('arxiv');
    page     = client.search(index=_index, scroll=str(int(_max_extract_time * _scroll_size)) + 'm', size=_scroll_size, query={"ids":{"values":IDs}})
    sid      = page['_scroll_id']
    returned = len(page['hits']['hits'])
    page_num = 0
    while returned > 0:
        for doc in page['hits']['hits']:
            #-------------------------------------------------------------------------------------------------------------------------------------------
            _id                               = doc['_id'];
            print('updating',doc['_id']);
            title                             = doc['_source']['title'] if 'title' in doc['_source'] else '';
            handle                            = _id;
            if HANDLE:
                try:
                    handle = int(_id.split('-')[-1]); print('#######################################################################',handle,'######');
                except:
                    print('\n[!]-----> Could not extract handle from', _id,'\n');
                    handle = None;
            if ARXIV:
                try:
                    handle = _id.replace('/','_');
                except:
                    print('Could not extract handle from', _id);
                    handle = None;
            address                                 = _addr.replace('#######',str(handle)) if handle != None else None;
            skipped                                 = 'bibliographie' in title.lower(); print(skipped,address)
            success                                 = download(address,_tmpdir+_pdfname+'_'+str(worker)+'.pdf') if (not skipped) and address!=None else False;
            xml                                     = extract(_tmpdir+_pdfname+'_'+str(worker)) if success else None;
            body                                    = copy(_body);
            body['_id']                             = _id;
            body['_source']['doc']['xml']           = xml;
            body['_source']['doc']['has_xml']       = True if xml else False;
            body['_source']['doc']['processed_xml'] = True; #TODO: Figure it out
            #-------------------------------------------------------------------------------------------------------------------------------------------
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

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# THE BULK UPDATING PROCESS
ut.process(compute, _index, list(ut.make_batches(_index,_scr_query,_max_extract_time,_scroll_size,_max_scroll_tries)), _chunk_size, _request_timeout, _workers)
#-------------------------------------------------------------------------------------------------------------------------------------------------
