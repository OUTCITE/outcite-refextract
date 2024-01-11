#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
import urllib.request
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from PyPDF2 import PdfFileWriter, PdfFileReader
from pathlib import Path
import M_utils as ut
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# THE ELASTICSEARCH INDEX WHERE WE WANT TO ADD THE PDF ADDRESS TO
_index = sys.argv[1];

# HTTP PORT WHERE THE PDFS ARE PROVIDED
_httport = sys.argv[2] if len(sys.argv) >= 3 else '8000';

# LOADING CONFIGS FROM FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

_recheck          = _configs['recheck_pdfs'];
_redownload       = _configs['redownload_pdfs'];

_pdfdir           = str((Path(__file__).parent / '../pdfs/ARXIV/').resolve())+'/' if _index.startswith('arxiv') else str((Path(__file__).parent / '../pdfs/USERS/').resolve())+'/' if _index=='users' else str((Path(__file__).parent / '../pdfs/GEOCITE/').resolve())+'/' if _index=='geocite' else str((Path(__file__).parent / '../pdfs/CIOFFI/').resolve())+'/' if _index=='cioffi' else str((Path(__file__).parent / '../pdfs/SSOAR/').resolve())+'/';#sys.argv[2:];
_addr             = 'https://arxiv.org/pdf/#######' if _index.startswith('arxiv') else 'http://svko-outcite.gesis.intra:'+_httport+'/UPLOADS/#######.pdf' if _index=='users' else 'http://svko-outcite.gesis.intra:'+_httport+'/GEOCITE/#######.pdf' if _index=='geocite' else 'http://svko-outcite.gesis.intra:'+_httport+'/CIOFFI/#######.pdf' if _index=='cioffi' else 'https://www.ssoar.info/ssoar/bitstream/handle/document/#######/?sequence=1';#sys.argv[3];
_chunk_size       = _configs['chunk_size_pdfs'];
_scroll_size      = _configs['scroll_size_pdfs'];
_max_extract_time = _configs['max_extract_time_pdf'];
_max_scroll_tries = _configs['max_scroll_tries_pdfs'];
_request_wait     = _configs['request_wait_pdf'];
_request_timeout  = 60;

# THE STRUCTURE OF THE BODY THAT IS USED IN THE BULK UPDATES
_body = { '_op_type': 'update',
          '_index': _index,
          '_id': None,
          '_source': {'doc': { 'pdf': 'http://svko-outcite.gesis.intra:'+_httport+'/'+'SSOAR/#######.pdf' if _index.endswith('ssoar') or _index.startswith('ssoar') else 'http://svko-outcite.gesis.intra:'+_httport+'/ARXIV/#######.pdf' if _index.startswith('arxiv') else 'http://svko-outcite.gesis.intra:'+_httport+'/'+_index.upper()+'/#######.pdf' if _index!='users' else 'https://demo-outcite.gesis.org/users/_pdf/#######.pdf'} }
        }

# THE QUERY THAT IS USED TO ITERATE OVER THE DOCUMENTS THAT SHOULD BE UPDATED
_scr_query = {'match_all':{}} if _recheck else {'bool':{'must_not':[{'term':{'processed_pdf': True}}]}};
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# DOWNLOADS AND POSSIBLY CROPS A PDF FROM A GIVEN ADDRESS TO A FOLDER THAT IS SERVICED BY HTTP SERVER
def download(address,filename,CROP,ARXIV=False):
    if os.path.isfile(filename) and not _redownload:
        print('Concerning pdf at',address,':', filename,'already exists.');
        return True;
    print('Trying to download',address,'to',filename,end='\r');
    success = True;
    try:
        pdf_file, headers = urllib.request.urlretrieve(address,filename)
        PDF               = open(pdf_file); PDF.close();
    except Exception as exception:
        print(exception);
        print('Failed to download',address,'to',filename);
        success = False;
    if ARXIV and success:
        time.sleep(_request_wait);
    if CROP and success:
        try:
            IN  = PdfFileReader(pdf_file, 'rb'); #TODO: For some reason this works even if the file is not pdf but xml or so
            OUT = PdfFileWriter();
            for i in range(1,IN.getNumPages()):
                OUT.addPage(IN.getPage(i));
            PDF =  open(filename, 'wb');
            OUT.write(PDF); PDF.close();
        except:
            print('Failed to cut off first page from',filename);
            success = False;
    if success:
        print('Successfully downloaded',address,'to',filename);
    return success;

# THE MAIN FUNCTION TO UPDATE A PDF ADDRESS TO DOWNLOAD LOCATION
def get_pdfs(HANDLE=False,CROP=False,ARXIV=False):
    client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=_index,scroll=str(int(1+_scroll_size*_max_extract_time))+'m',size=100,query=_scr_query);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            print('updating',doc['_id']);
            #-------------------------------------------------------------------------------------------------------------------------
            _id    = doc['_id'];
            handle = doc['_source']['id'] if ARXIV else _id;
            if HANDLE:
                try:
                    handle = int(_id.split('-')[-1]);
                except:
                    print('Could not extract handle from', _id);
                    handle = None;
            if ARXIV:
                try:
                    handle = ut.ortho_paper_id(handle,True);#_id.replace('/','_');
                except:
                    print('Could not extract handle from', _id);
                    handle = None;
            address                                 = _addr.replace('#######',str(handle)) if handle and not ARXIV else _addr.replace('#######',str(_id)) if ARXIV else None;
            success                                 = download(address,_pdfdir+str(handle)+'.pdf',CROP,ARXIV) if address else False;
            body                                    = copy(_body);
            body['_id']                             = _id;
            body['_source']['doc']['pdf']           = body['_source']['doc']['pdf'].replace('#######',str(handle)) if success else None;
            body['_source']['doc']['has_pdf']       = success;
            body['_source']['doc']['processed_pdf'] = True;
            #-------------------------------------------------------------------------------------------------------------------------
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(1+_scroll_size*_max_extract_time))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('WARNING: Some problem occured while scrolling. Sleeping for 3s and retrying...');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client = ES(['http://localhost:9200'],timeout=60);

# THE BULK UPDATING PROCESS
i = 0;
for success, info in bulk(_client,get_pdfs((not _index.startswith('arxiv')) and _index!='users' and _index!='geocite' and _index!='cioffi',(not _index.startswith('arxiv')) and _index!='users' and _index!='geocite' and _index!='cioffi',_index.startswith('arxiv')),chunk_size=_chunk_size, request_timeout=_request_timeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
