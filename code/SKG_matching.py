# -*- coding: utf-8 -*-
#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
from elasticsearch import Elasticsearch as ES
import re
import time
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

_gate        = 'search.gesis.org'; #svko-exploredata-staging'
_index       = 'gesis'; #'explore_data'
_port        = 9200; #9200
_timeout     = 10;
_retry_time  = 5;

_minscore   = 10; # was 12 at some point!
_minchange  = 0.90;
_maxresults = 20;
_resultsize = 20;

ZA_NUMBER     = re.compile(r'\bZA[1-9][0-9]{3}\b');
EUROBAROMETER = re.compile(r'\bEB\b');
EVS           = re.compile(r'\bEVS\b');
ALLBUS        = re.compile(r'ALLBUS');
KUM1          = re.compile(r'kum1');
KUM2          = re.compile(r'kum2');

replacements = [(EUROBAROMETER,'Eurobarometer '),(EVS,'EVS European Value Study '),(ALLBUS,'Allbus '),(KUM1,'Cumulation 1'),(KUM2,'Cumulation 2')];
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def match(doi_field,doi_val,id_field,id_val,title_field,title_val,toType,GWS,use_title=True,to_index=_index,port=_port,gate=_gate):
    client = _client if GWS else ES(['http://'+gate+':'+str(port)],timeout=60);#ES([gate],scheme='http',port=port,timeout=_timeout_src);
    if title_val != None:
        for regex, replacement in replacements:
            title_val = regex.sub(replacement,title_val);
        for ZA in ZA_NUMBER.findall(title_val):
            title_val = title_val.replace(ZA,'');
            id_val    = ZA if id_val==None else id_val; # If there should be multiple ZA-Numbers for some reason only the last one will be chosen. Also this was doi_val before, but why?
    body = {"query":{"term":{id_field+['','.keyword'][GWS]:id_val}}} if id_val else {"query":{ "match":{doi_field:doi_val}}} if doi_val else {"query":{ "match_phrase":{title_field:title_val}}} if title_val and use_title else None;
    if body == None:
        return [];
    response = None;
    while True:
        try:
            response = client.search(index=to_index,size=_resultsize,body=body,_source=[id_field,doi_field,title_field,'type']);
            break;
        except:
            print(body);
            print(sys.exc_info()[0]);
            print('Probably timeout. Retrying...');
            time.sleep(_retry_time);
    results = [];
    if response:
        results = [ ( response['hits']['hits'][i]['_source'][id_field][0]                     if id_field    in response['hits']['hits'][i]['_source'] and isinstance(  response['hits']['hits'][i]['_source'][id_field]   ,list) else response['hits']['hits'][i]['_source'][id_field]                     if id_field    in response['hits']['hits'][i]['_source'] else None,                                                                 # ID
                      response['hits']['hits'][i]['_source'][doi_field][0]                    if doi_field   in response['hits']['hits'][i]['_source'] and isinstance(  response['hits']['hits'][i]['_source'][doi_field]  ,list) else response['hits']['hits'][i]['_source'][doi_field]                    if doi_field   in response['hits']['hits'][i]['_source'] else None,                                                                 # DOI
                      response['hits']['hits'][i]['_source'][title_field][0].replace('\n','') if title_field in response['hits']['hits'][i]['_source'] and isinstance(  response['hits']['hits'][i]['_source'][title_field],list) else response['hits']['hits'][i]['_source'][title_field].replace('\n','') if title_field in response['hits']['hits'][i]['_source'] else None, # TITLE
                      response['hits']['hits'][i]['_score'],                                                                                                                                                                                       # SCORE
                      response['hits']['hits'][i]['_score']/float(response['hits']['hits'][max(0,i-1)]['_score']),                                                                                                                                 # RELATIVE SCORE
                      response['hits']['hits'][i]['_source']['type']                          if 'type'      in response['hits']['hits'][i]['_source']                                                                            else None,
                      to_index,
                      response['hits']['hits'][i]['_source']['date']                          if 'date'      in response['hits']['hits'][i]['_source']                                                                            else None
                    ) for i in range(len(response['hits']['hits'])) ];
        #print(body);
        #print(response);
        #print('--------------------------');
        #for result in results:
        #    print(result);
        #print('--------------------------');
        results = clean(results,_maxresults,_minscore,_minchange,toType);#,date);
        #print('--------------------------');
        #for result in results:
        #    print(result);
        #print('--------------------------');
    return results;

def clean(results,maxresults,minscore,minchange,toType):#,date): #TODO: Cannot check the linked dataset publication year is not greater than the publication's year because might not be fetched
    valid = [];
    for i in range(len(results)):
        if i < maxresults and results[i][3] > minscore and results[i][4] >= minchange:
            if (results[i][0] != None or results[i][1] != None) and (results[i][5] == None or results[i][5] == toType) and (results[i][6] != 'sowiport' or len(results)==1 or results[i][0].startswith('gesis-solis-')):
                #date_ok = False;
                #try: #TODO: Could be improved by parsing the field, but usually it is a year only anyway
                #    date_from, date_to = int(date), int(results[i][7]);
                #except:
                #    print('Could not get integer of',date,'or',results[i][7],'or both.');
                #    date_ok = True;
                #if not date_ok:
                #    date_ok = date_from >= date_to;
                #if date_ok:
                valid.append(results[i]);
        else:
            break;
    return valid;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['http://'+_gate+':'+str(_port)],timeout=60);#ES([_gate],scheme='http',port=_port,timeout=_timeout);
#-------------------------------------------------------------------------------------------------------------------------------------------------
