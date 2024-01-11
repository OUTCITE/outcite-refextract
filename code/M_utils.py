# -IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import json
import os, sys
import re
from pathlib import Path
import subprocess
import requests
import urllib.request
from bs4 import BeautifulSoup
import time
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from difflib import SequenceMatcher as SM
from datetime import datetime
import multiprocessing as mp
import random
# -------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# LOADING CONFIGS FROM FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs.json');
_configs = json.load(IN);
IN.close();

# REGEXED FOR BIBTEX ENTRIES, AUTHORS, URLS AND DOIS
_BIB_entries = re.compile(r'^@',re.MULTILINE);
_AUTHOR      = re.compile(r"(^|, )[A-Z][A-Za-züäöß\-']+,(( )*[A-Z](\.|[A-Za-züäöß\-']+))+");
_URL         = re.compile(_configs['regex_url']); #re.compile(r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))(([\w.\-\/,@?^=%&:~+#]|([\.\-\/=] ))*[\w@?^=%&\/~+#])');
_DOI         = re.compile(_configs['regex_doi']); #re.compile(r'((https?:\/\/)?(www\.)?doi.org\/)?10.\d{4,9}\/[-._;()\/:A-Z0-9]+');

# TYPE MAPPING FOR ANYSTYLE <-> COMMON DATA MODEL
_type2type = { 'chapter':          'book-chapter',
               'article-journal':  'journal-article',
               'webpage':          'posted-content',
               'thesis':           'dissertation',
               'paper-conference': 'proceedings-article' };

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# CHECKING URLS IF THEY ARE FOUND IN THE REFERENCE AS SUCH
def check(url,RESOLVE=False,cur=None,timeout=5):
    print('Checking URL',url,'...');
    page   = None;
    status = None;
    try:
        status = None;
        if cur:
            rows    = cur.execute("SELECT status,resolve FROM urls WHERE url=?",(url,)).fetchall();
            status  = rows[0][0] if rows and rows[0] else None;
            new_url = rows[0][1] if rows and rows[0] else None;
        if not status:
            page    = requests.head(url,allow_redirects=True,timeout=timeout) if RESOLVE else requests.head(url,timeout=timeout);
            status  = page.status_code;
            new_url = page.url;
            if cur:
                cur.execute("INSERT INTO urls VALUES(?,?,?)",(url,status,new_url,));
        if status in [400,404]+list(range(407,415))+list(range(500,511)):
            print('----> Could not resolve URL due to',status,url);
            return None;
    except Exception as e:
        print('ERROR:',e, file=sys.stderr);
        print('----> Could not resolve URL due to above exception',url);
        return None;
    if new_url:
        print('Successfully resolved URL',url,'to',new_url);
    else:
        print('----> Could not resolve URL for some reason',url,'-- status:',status);
    return new_url if RESOLVE else url;

# MATCHING FUNCTIONS FOR RETRIEVING REFERENCE OBJECTS BY MATCHING DOCUMENT METADATA TO CROSSREF OR OPENALEX METADATA
def distance(a,b):
    a,b        = a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    return 1-(overlap / max([len(a),len(b)]));

def compare(title_found,title_searched,max_rel_diff):
    title_found = title_found[0] if isinstance(title_found,list) and title_found else title_found if isinstance(title_found,str) else '';
    print(title_found,'<--->',title_searched)
    dist = distance(title_found,title_searched);
    OK   = dist < max_rel_diff; print(OK,dist)
    return OK;

def norm_doi(doi):
    return doi.split('doi.org/')[-1];

def get_matched_refs(doi,title,client,index_match,max_rel_diff,ref_field,norm_func):
    query   = {'term':{'doi':norm_doi(doi)}} if doi else {'match':{'title':title}};
    results = client.search(index=index_match, query=query)['hits']['hits'];
    doc     = results[0]['_source'] if len(results) >= 1 else None;
    OK      = compare(doc['title'],title,max_rel_diff) if title and doc and 'title' in doc and doc['title'] else False;
    if not OK:
        return [], True;
    references = doc[ref_field] if ref_field in doc and doc[ref_field] else [];
    references = [norm_func(reference) for reference in references];
    return references, True;

def update_matched_references(index,index_m,scr_query,body_,max_extract_time,scroll_size,max_scroll_tries,max_rel_diff,ref_field,norm_func,output_field): #TODO: What is this doing here?
    output_indicator   = 'processed_'+output_field;
    output_min1        = 'has_'      +output_field;
    output_field_count = 'num_'      +output_field;
    client             = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
    client_m           = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
    page               = client.search(index=index, scroll=str(int(max_extract_time * scroll_size)) + 'm', size=scroll_size, query=scr_query)
    sid                = page['_scroll_id']
    returned           = len(page['hits']['hits'])
    page_num           = 0
    while returned > 0:
        for doc in page['hits']['hits']:
            print('updating',doc['_id']);
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            #TODO: So far, there will never be a doi, because this still needs to be added during indexing!
            title                                      = doc['_source']['title'] if '_source' in doc and 'title' in doc['_source'] and doc['_source']['title'] else None
            doi                                        = doc['_source']['doi'  ] if '_source' in doc and 'doi'   in doc['_source'] and doc['_source']['doi']   else None
            refobjs, success                           = get_matched_refs(doi,title,client_m,index_m,max_rel_diff,ref_field,norm_func) if doi or title else ([], False)
            body                                       = copy(body_)
            body['_id']                                = doc['_id']
            body['_source']['doc'][output_field]       = refobjs
            body['_source']['doc'][output_indicator]   = success
            body['_source']['doc'][output_min1]        = len(refobjs)>0 if success else False
            body['_source']['doc'][output_field_count] = len(refobjs)   if success else 0
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            yield body
        scroll_tries = 0
        while scroll_tries < max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(max_extract_time * scroll_size)) + 'm')
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

# HELPER FUNCTION TO FIX CHANGED ARXIV IDS
def ortho_paper_id(paper_id, for_save=False): # BY LU GAN
    if for_save:
        if '/' in paper_id:
            paper_id = paper_id.split('/')
            return '.'.join(paper_id)
        return paper_id
    else:
        # print(type(paper_id), paper_id)
        if '.' in paper_id:
            paper_id = paper_id.split('.')
            if len(paper_id[0]) == 3:
                paper_id[0] = '0'    + paper_id[0]
            if len(paper_id[1]) == 4:
                paper_id[1] = '0'    + paper_id[1]
            elif len(paper_id[1]) == 3:
                paper_id[1] = '00'   + paper_id[1]
            elif len(paper_id[1]) == 2:
                paper_id[1] = '000'  + paper_id[1]
            elif len(paper_id[1]) == 1:
                paper_id[1] = '0000' + paper_id[1]
            return '.'.join(paper_id)
        return paper_id

# PARSING BIBTEXT TO COMMON DATA MODEL
def parse_bibtex(bib):
    entries = [el.rstrip() for el in re.split(_BIB_entries,bib) if not el==''];
    D       = [];
    for entry in entries:
        #print('>>>> entry:', entry)
        d         = dict();
        lines     = [line.rstrip() for line in entry.split('\n')];# print('>>>> lines:', lines)
        typ, ID   = lines[0].split('{');
        d['type'] = typ;
        d['id']   = ID[:-1];     # assuming that all lines end on ','!!!
        for line in lines[1:-1]:
            parts = line[1:].split(' = ');
            if len(parts)==2:
                key, val = parts;
                d[key] = val[1:-2]; # assuming that all lines end on ','!!!
        #print('-------------------------------------')
        #for key in d:
        #    print(key,':',d[key])
        #print('-------------------------------------')
        #doi_scale = '/'.join(d['ref_doi_scale'].split('/')[-2:]);
        #d['ref_id'] = doi_scale+'_'+d['ref_id'];
        #if doi_scale in D:
        #    D[doi_scale].append(d);
        #else:
        #    D[doi_scale] = [d];
        D.append(d);
    return D;

# HELPER FUNCTION BY AHSAN
def clean_and_split_text(text, parse_type): #TODO: Makes no sense if text is int then the thing would fail
    if not text:
        return [];
    clean_text = text.translate({ord(c): " " for c in "–!-@#$%^&*()[]{};:,./<>?\|`~-=_+"}) if not isinstance(text, int) else text
    split_text = [int(s) for s in clean_text.split() if s.isdigit() and int(s) <= sys.maxsize] if parse_type == 'int' else clean_text.split() # check if number is in range of long (-9223372036854775808, 9223372036854775807)
    return split_text

# DOWNLOAD PDF TO LOCAL FOLDER
def download_pdf(address, filename):
    print('[-]-----> Trying to download', address, 'to', filename)
    success = False
    try:
        urllib.request.urlretrieve(address, filename)
        success = True
    except Exception as e:
        print(e)
        print('\n[!]-----> Failed to download', address)
    if success:
        print('[o]-----> Successfully downloaded', address)
    return success

# GENERIC HELPER FUNCTION TO CALL A PROGRAM AND OBSERVE THE RESULT ON DISK
def obtain_results(call,resultfiles=[]):
    results, success = [],True
    for resultfile in resultfiles:
        if os.path.exists(resultfile):
            os.remove(resultfile)
    try:
        results = [subprocess.check_output(call).decode()];
    except Exception as e:
        success = False;
        print('Failed to run %s. Reason: %s' % (' '.join(call), e))
    if success and resultfiles:
        results = [];
        for resultfile in resultfiles:
            if os.path.exists(resultfile):
                IN    = open(resultfile);
                INPUT = None;
                try:
                    INPUT = IN.read();
                except:
                    print('Failed to read from file',resultfile, 'probably due to encoding problem');
                    success = False;
                IN.close();
                if INPUT:
                    results.append(INPUT);
            else:
                print('Failed to open file',resultfile, 'as it odes not exist.');
                success = False;
    return results, success;

# REMOVE ALL KEY VALUE PAIRS FROM DICTIONARY WHERE THE VALUE IS NONE OR ALL NONE FROM LIST
def clean_nones(value):
    if isinstance(value, list):
        return [clean_nones(x) for x in value if x is not None]
    elif isinstance(value, dict):
        return {
            key: clean_nones(val)
            for key, val in value.items()
            if val is not None
        }
    else:
        return value

# CERMINE REFERENCE OBJECTS TO COMMON DATA MODEL
def cermine_map(refobjs,refstrings):
    references = [];
    for i in range(len(refobjs)):
        ref = {'reference': refstrings[i]};
        #------------------------------------------------------------------------------------------------------------------------------------------------
        for target,source in [('title','title'),('doi','doi'),('source','journal')]:
            ref[target] = refobjs[i][source] if source in refobjs[i] else None;
        #------------------------------------------------------------------------------------------------------------------------------------------------
        for target,source in [('year','year'),('volume','volume'),('issue','number')]:
            values      = clean_and_split_text(refobjs[i][source], 'int') if source in refobjs[i] and isinstance(refobjs[i][source],str) else [refobjs[i][source]] if source in refobjs[i] and isinstance(values,int) else [];
            ref[target] = values[0] if len(values) > 0 else None;
        #------------------------------------------------------------------------------------------------------------------------------------------------
        pages                    = clean_and_split_text(refobjs[i]['pages'], 'int') if 'pages' in refobjs[i] else [];
        ref['start'], ref['end'] = (pages[0],None) if len(pages)==1 else (pages[0],pages[1]) if len(pages) > 1 and pages[0] < pages[1] else (None,None)
        #------------------------------------------------------------------------------------------------------------------------------------------------
        authors        = [group[0][2:] if group[0].startswith(', ') else group[0] for group in _AUTHOR.finditer(refobjs[i]['author'])] if 'author' in refobjs[i] and refobjs[i]['author'] else [];
        ref['authors'] = [{'author_string':author} for author in authors];
        #------------------------------------------------------------------------------------------------------------------------------------------------
        editors        = [group[0][2:] if group[0].startswith(', ') else group[0] for group in _AUTHOR.finditer(refobjs[i]['editor'])] if 'editor' in refobjs[i] and refobjs[i]['editor'] else [];
        ref['editors'] = [{'editor_string':editor} for editor in editors];
        #------------------------------------------------------------------------------------------------------------------------------------------------
        ref['publishers'] = [{'publisher_string':refobjs[i]['publisher'][0]}] if 'publisher' in refobjs[i] and refobjs[i]['publisher'] else [];
        #------------------------------------------------------------------------------------------------------------------------------------------------
        references.append(ref);
    return references;

# ANYSTYLE REFERENCE OBJECTS TO COMMON DATA MODEL
def anystyle_map(refobjs,refstrings,cur=None):
    references = [];
    for i in range(len(refobjs)):
        ref = {'reference': refstrings[i]};
        #------------------------------------------------------------------------------------------------------------------------------------------------
        for target,source in [('type','type')]:
            ref[target] = refobjs[i][source] if source in refobjs[i] else None;
        #------------------------------------------------------------------------------------------------------------------------------------------------
        for target,source in [('title','title'),('place','location'),('source','container-title'),('doi','doi'),('url','url')]:
            ref[target] = refobjs[i][source][0] if source in refobjs[i] and refobjs[i][source] else None;
        #------------------------------------------------------------------------------------------------------------------------------------------------
        for target,source in [('year','date'),('volume','volume'),('issue','issue')]:
            #values      = clean_and_split_text(refobjs[i][source][0],'int') if source in refobjs[i] and refobjs[i][source] else None;
            values      = clean_and_split_text(refobjs[i][source][0], 'int') if source in refobjs[i] and refobjs[i][source] and isinstance(refobjs[i][source][0],str) else [refobjs[i][source][0]] if source in refobjs[i] and refobjs[i][source] and isinstance(refobjs[i][source][0],int) else [];
            ref[target] = values[0] if values else None;
        #------------------------------------------------------------------------------------------------------------------------------------------------
        for target,source in [('editors','editor'),('authors','author')]:
            ref[target] = [extract_names(person,source) for person in refobjs[i][source]] if source in refobjs[i] else [];
        #------------------------------------------------------------------------------------------------------------------------------------------------
        pages        = clean_and_split_text(refobjs[i]['pages'][0],'int') if 'pages'  in refobjs[i] and refobjs[i]['pages'] else [];
        pages        = None     if len(pages) > 1 and pages[0] > pages[1] else pages; # page start must be smaller than page end
        ref['start'] = pages[0] if pages                                  else None;
        ref['end']   = pages[1] if pages and len(pages)>1                 else None;
        #------------------------------------------------------------------------------------------------------------------------------------------------
        ref['publishers'] = [{'publisher_string':refobjs[i]['publisher'][0]}] if 'publisher' in refobjs[i] and refobjs[i]['publisher'] else [];
        #------------------------------------------------------------------------------------------------------------------------------------------------
        ref['type'] = _type2type[ref['type']] if ref['type'] in _type2type else ref['type'];
        #------------------------------------------------------------------------------------------------------------------------------------------------
        urls = [];
        for url_ in [match.group(0) for match in _URL.finditer(ref['reference'])]:
            url_s = url_.split(' ');
            longest_url = None;
            for i in range(len(url_s)):
                url         = ''.join(url_s[:i+1]);
                url         = url[:-1] if url.endswith('.') else url;
                url         = check(url,False,cur,3);
                longest_url = url if url else longest_url;
            urls = urls + [longest_url] if longest_url and not (('doi' in ref and ref['doi'] and ref['doi'].lower() in longest_url.lower()) or ('arxiv' in ref and ref['arxiv'] and ref['arxiv'].lower() in longest_url.lower())) else urls;
        if urls:
            ref['url'] = urls[0]; # There are hardly ever multiple URLS in a reference string
            print('url',urls[0])
        #------------------------------------------------------------------------------------------------------------------------------------------------
        references.append(clean_nones(ref));
    return references;

# CERMINE OR GROBID REFERENCE XML TO REFERENCE STRINGS
def xml2refstrs(xml,source):
    refstrs      = []
    citation_ids = None
    soup         = BeautifulSoup(xml, "xml")
    if source == 'cermine': #TODO: We may want to check if cermine also has citation identifiers
        root     = soup.find('article')
        back_tag = root.find('back') if root else ''
        refobjs  = back_tag.find('ref-list').find_all('ref') if back_tag else []
        for refobj in refobjs:
            raw_ref = re.sub('<.*?>', '', str(refobj)).split()
            refstr  =  ' '.join(raw_ref).replace(' .', '.').replace(' ,', ',').replace(' -', '-').replace('- ', '-').replace('( ', '(').replace(' )', ')')
            refstrs.append(refstr)
    elif source == 'grobid':
        citation_ids = []
        root         = soup.find('TEI')
        text         = root.findChildren('text', recursive='false') if root else []
        for children in text:
            refobjs = children.find('back').find('listBibl').find_all('biblStruct')
            if refobjs:
                for refobj in refobjs:
                    citation_id = refobj.get('xml:id')
                    notes       = refobj.find_all('note')
                    for note in notes:
                        if note.get('type') == 'raw_reference':
                            refstrs.append(note.text) #TODO: Why pick the last one only before??
                            citation_ids.append(citation_id)
    return refstrs, citation_ids

# CERMINE REFERENCE OBJECTS TO COMMON DATA MODEL
def cermine_xml_to_refobjs(xml): #TODO: Can it also not succeed?
    ref_list = []
    soup = BeautifulSoup(xml, "xml")
    root = soup.find('article')
    back_tag = root.find('back') if root else ''
    all_refs = back_tag.find('ref-list').find_all('ref') if back_tag else []
    for each_ref in all_refs:
        title = each_ref.find('article-title')
        authors = each_ref.find_all('string-name')
        date = each_ref.find('year')
        sources = each_ref.find_all('source')
        fpages = each_ref.find_all('fpage')
        lpages = each_ref.find_all('lpage')
        volumes = each_ref.find_all('volume')
        issues = each_ref.find_all('issue')
        ref = dict()
        raw_ref = re.sub('<.*?>', '', str(each_ref))
        raw_ref = raw_ref.split()
        ref['reference'] = ' '.join(raw_ref).replace(' .', '.').replace(' ,', ',').replace(' -', '-').replace('- ', '-').replace('( ', '(').replace(' )', ')')
        if authors:
            ref['authors'] = []
            for author in authors:
                auth = dict()
                given_names = author.find('given-names')
                if given_names:
                    auth['initials'] = []
                    auth['author_string'] = ''
                    cleaned_split_fnames = clean_and_split_text(given_names.text, 'str')
                    for each_fname in cleaned_split_fnames:
                        if len(each_fname) > 1:
                            if 'firstnames' not in auth:
                                auth['firstnames'] = []
                            auth['firstnames'].append(each_fname)
                        auth['initials'].append(each_fname[0])
                        auth['author_string'] = ' '.join([auth['author_string'], each_fname]) if auth['author_string'] != '' else each_fname
                surname = author.find('surname')
                if surname:
                    if 'author_string' not in auth:
                        auth['author_string'] = ''
                    # auth['author_type'] = 'Person'
                    auth['surname'] = author.find('surname').text
                if 'surname' in auth and (auth['surname'] not in auth['author_string']):  # for appending surname at end of string
                    auth['author_string'] = ' '.join([auth['author_string'], auth['surname']]) if auth['author_string'] != '' else auth['surname']
                    # auth['author_string'] += auth['surname']
                ref['authors'].append(auth)
        if title:
            ref['title'] = title.text
        if sources:
            ref['source'] = sources[0].text
        if date:
            cleaned_split_date = clean_and_split_text(date.text, 'int')
            if cleaned_split_date:
                ref['year'] = cleaned_split_date[0]
        if fpages:
            cleaned_split_fpage = clean_and_split_text(fpages[0].text, 'int')
            if cleaned_split_fpage:
                ref['start'] = cleaned_split_fpage[0]
                if lpages:
                    cleaned_split_lpage = clean_and_split_text(lpages[0].text, 'int')
                    if cleaned_split_lpage and cleaned_split_fpage[0] < cleaned_split_lpage[0]:  # page start must be smaller than page end
                        ref['end'] = cleaned_split_lpage[0]
        if volumes:
            cleaned_split_vol = clean_and_split_text(volumes[0].text, 'int')
            if cleaned_split_vol:
                ref['volume'] = cleaned_split_vol[0]
        if issues:
            cleaned_split_issue = clean_and_split_text(issues[0].text, 'int')
            if cleaned_split_issue:
                ref['issue'] = cleaned_split_issue[0]
        ref_list.append(ref)
    return ref_list, True

# HELPER FUNCTION BY AHSAN
def extract_names(obj, role):
    person = dict()
    for name_type in obj:
        if name_type == 'given':
            person['initials'] = []
            person[role + '_string'] = ''
            cleaned_split_fnames = clean_and_split_text(obj[name_type], 'str')
            for each_fname in cleaned_split_fnames:
                if len(each_fname) > 1:
                    if 'firstnames' not in person:
                        person['firstnames'] = []
                    person['firstnames'].append(each_fname)
                person['initials'].append(each_fname[0])
                person[role + '_string'] = ' '.join([person[role + '_string'], each_fname]) if person[role + '_string'] != '' else each_fname
        elif name_type == 'family':
            if role + '_string' not in person:
                person[role + '_string'] = ''
            person['surname'] = obj[name_type]
        else:
            if role + '_string' not in person:
                person[role + '_string'] = obj[name_type]
    if 'surname' in person and (person['surname'] not in person[role + '_string']):  # for appending surname at end of string
        person[role + '_string'] = ' '.join([person[role + '_string'], person['surname']]) if person[role + '_string'] != '' else person['surname']
    person[role+'_string'] = None if role+'_string' not in person or not isinstance(person[role+'_string'],str) else person[role+'_string']; #There was somewhere Boolean returned, no idea why...
    person['surname']      = None if 'surname'      not in person or not isinstance(person['surname'     ],str) else person['surname'];
    person['initials']     = [] if 'initials' not in person else [initial for initial in person['initials'] if isinstance(initial,str)];
    return person

# PARALLELISATION HELPER FUNCTIONS
def put(value, queue, sleeptime=0.1, max_trytime=1):  # To put something into a queue
    start_time = time.time()
    try_time = 0
    while True:
        try:
            queue.put(value, block=False)
            break
        except Exception as e:
            try_time = time.time() - start_time
            if try_time > max_trytime:
                return 1
            time.sleep(sleeptime)

def get(queue, sleeptime=0.02, max_trytime=0.1):  # To get something from a queue
    start_time = time.time()
    try_time = 0
    value = None
    while True:
        try:
            value = queue.get(block=False)
            break
        except Exception as e:
            try_time = time.time() - start_time
            if try_time > max_trytime:
                break
            time.sleep(sleeptime)
    return value

def queue2list(Q):  # This takes a queue and gets all the results as a list
    L = []
    while True:
        element = get(Q)
        if element is None:
            break
        L.append(element)
    return L

def queue2iterator(queue):  # This takes a queue and gets all the results as a generator/iterator
    while True:
        element = get(queue, 0.5, 60)
        if element is None:
            break
        yield element

def join(workers):  # Function that tries to join workers (check if a worker has terminated otherwise proceed to check on another one, essentially waits for them to finish)
    to_join = set(range(len(workers)))
    while len(to_join) > 0:
        i = random.sample(to_join, 1)[0]
        workers[i].join(0.05)
        if not workers[i].is_alive():
            to_join.remove(i)
            print(len(to_join), 'workers left to join.', end='\r')
        else:
            time.sleep(0.01)

def start(workers, batches, Q):  # Put the batches into the job queue and start all workers
    for batch in batches:
        put(batch, Q)
    for worker in workers:
        worker.start()

def make_batches(index,scr_query,max_extract_time,scroll_size,max_scroll_tries):  # E.g. yield the ids of the records in the index that match the query
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
    page     = client.search(index=index, scroll=str(int(max_extract_time * scroll_size)) + 'm', size=scroll_size, query=scr_query)
    sid      = page['_scroll_id']
    returned = len(page['hits']['hits'])
    page_num = 0
    while returned > 0:
        for doc in page['hits']['hits']:
            print('going to update',doc['_id']);
            yield tuple([doc['_id']])
        scroll_tries = 0
        while scroll_tries < max_scroll_tries:
            try:
                page = client.scroll(scroll_id=sid, scroll=str(int(max_extract_time * scroll_size))+'m')
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

def output(R,index_,chunk_size,request_timeout):
    index(index_,queue2iterator(R),chunk_size,request_timeout);

def work(function,Q,R,worker):  # Where the parallelized work is done
    client = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
    while True:
        IDs = get(Q)
        if IDs:  # None means there could not be gotten anything from the queue
            for result in function(IDs,client,worker):
                put(result, R)
        else:
            break

def process(function, index_, batches, chunk_size, request_timeout, num_workers=4):  # Runs the workers and the returner in parallel
    if num_workers == 1: #TODO: This does not seem to actually update the index?
        client = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
        bodies = [result for IDs in batches for result in function(IDs,client,0)];
        index(index_,bodies,chunk_size,request_timeout);
    else:
        print(len(batches), '---', sys.getsizeof(batches))
        current_time = datetime.now().strftime("%H:%M:%S");     print("Processes Starting Time =", current_time)
        manager      = mp.Manager()
        Q, R         = manager.Queue(), manager.Queue()
        workers      = [mp.Process(target=work, args=(function,Q,R,x)) for x in range(num_workers)]
        returner     = mp.Process(target=output, args=(R,index_,chunk_size,request_timeout,))
        start(workers, batches, Q)
        returner.start()
        join(workers)
        join([returner])

# INDEX TO TARGET INDEX BASED ON QUEUE
def index(index_,bodies,chunk_size,request_timeout):  # Check the results, send the result to the index and as long as you do not have to wait very long for one to appear in R, then continue
    client = ES(['http://localhost:9200'],timeout=60);#ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
    print('-------------------------------------indexing started------------------------------------')
    # for body in queue2iterator(R):  # This is for testing what happens # TODO: comment out when applying
    #     print('body: ', body['_id'])
    # -----------------------------------------------------------------------------------------------------------------
    i = 0
    for success, info in bulk(client, bodies, chunk_size=chunk_size,request_timeout=request_timeout):
        i += 1
        print('######', i, '#######################################################################')
        if not success:
            print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'], '\n')
        if i % chunk_size == 0:  # TODO: Check if this actually works
            print(i, ' refreshing...')
            client.indices.refresh(index=index_)
            print(i, ' refreshed...!!!')
    client.indices.refresh(index=index_)
    print(i, ' Refreshed and Returner Process Ended...!!!')
# -------------------------------------------------------------------------------------------------------------------------------------------------
