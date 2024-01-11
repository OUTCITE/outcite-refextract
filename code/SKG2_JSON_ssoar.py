#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import json
import re
import sqlite3
from copy import deepcopy as copy
import collections
from collections import Counter
import SKG_matching as matcher
from SKG_common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_infile  = sys.argv[1];
_context = sys.argv[2];
_outfile = sys.argv[3];
_GWS     = len(sys.argv) > 4 and sys.argv[4].lower() == 'gws';

_empty  = set([None,'','n','X']);
_prefix = 'ssoar';

IN_context = open(_context);
_context = json.load(IN_context);
IN_context.close();
_initial = {'@id':'https://data.gesis.org/skg/ssoar','skg':{'@id':'https://data.gesis.org/skg/ssoar','@type':'GESIS_collection'}};

con = sqlite3.connect(_infile);
cur = con.cursor();

_index2column = [column.split()[0] for column in cur.execute("SELECT sql FROM sqlite_master WHERE tbl_name='rows' AND type=='table'").fetchall()[0][0].replace(')','').split('(')[1].split(', ')];
_column2index = {_index2column[i]:i for i in range(len(_index2column))};

_id_field     = 'id';
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-DATAFLOW----------------------------------------------------------------------------------------------------------------------------------------
#----It is not possible to properly map values from two different fields into the same target if the value defines its own dictionary key!
#----E.g. topics_en and topics_de cannot be mapped to topics->''->term. Multiple origin fields can be mapped to same target in <source>_map.txt
#-------------------------------------------------------------------------------------------------------------------------------------------------
_targets = {
    'id':                      {'skg':{_prefix:{'':{'@type':'Publication','@id':None }}}},
    'doi':                     {'skg':{_prefix:{'':{'doi':                     None }}}},
    'uri':                     {'skg':{_prefix:{'':{'uri':                     None }}}},
    'urn':                     {'skg':{_prefix:{'':{'urn':                     None }}}},
    'issn':                    {'skg':{_prefix:{'':{'issn':                    None }}}},
    'isbn':                    {'skg':{_prefix:{'':{'isbn':                    None }}}},
    'url':                     {'skg':{_prefix:{'':{'url':                     None }}}},
    'handle':                  {'skg':{_prefix:{'':{'handle':                  None }}}},
    'version':                 {'skg':{_prefix:{'':{'version':                 None }}}},
    'title':                   {'skg':{_prefix:{'':{'title':                   None }, 'duplicates':{} }}},
    'title_alt':               {'skg':{_prefix:{'':{'title_alternative':       None }}}},
    'date_accession':          {'skg':{_prefix:{'':{'date_info': {'accession_date':    None }}}}},
    'date_available':          {'skg':{_prefix:{'':{'date_info': {'available_date':    None }}}}},
    'date_modified':           {'skg':{_prefix:{'':{'date_info': {'modification_date': None }}}}},
    'date_issued':             {'skg':{_prefix:{'':{'date_info': {'issue_date':        None }}}}},
    'year_issued':             {'skg':{_prefix:{'':{'pub_year':                None }}}},
    'publisher':               {'skg':{_prefix:{'':{'pub_info': {'publisher':           None }}}}},
    'publisher_country':       {'skg':{_prefix:{'':{'pub_info': {'publication_country': None }}}}},
    'publisher_city':          {'skg':{_prefix:{'':{'pub_info': {'publication_city':    None }}}}},
    'publication_group':       {'skg':{_prefix:{'':{'pub_info': {'publication_group':   None }}}}},
    'publication_status':      {'skg':{_prefix:{'':{'pub_info': {'publication_status': None }}}}},
    'contributor':             {'skg':{_prefix:{'':{'contributors': {'':{'@type':'Person','@id':None, 'name':None }}}}}},
    'contributor_email':       {'skg':{_prefix:{'':{'contributors': {'':{'email':None }}}}}},
    'contributor_institution': {'skg':{_prefix:{'':{'contributors': {'':{'institution':None }}}}}},
    'editor':                  {'skg':{_prefix:{'':{'editors':      {'':{'@type':'Person','@id':  None,'id_mention':None,'name':None,'surname':None,'firstnames':[],'firstinits':[] }}}}}},
    'corp_editor':             {'skg':{_prefix:{'':{'edit_corps':   {'':{'@type':'Institution','@id':  None,'id_mention':None,'name':None }}}}}},
    'author':                  {'skg':{_prefix:{'':{'authors':      {'':{'@type':'Person','@id':  None,'id_mention':None,'name':None,'surname':None,'firstnames':[],'firstinits':[] }}}}}},
    'affiliation':             {'skg':{_prefix:{'':{'affiliations': {'':{'@type':'Institution','@id':None, 'name':None }}}}}},
    'location':                {'skg':{_prefix:{'':{'locations':    {'':{'@type':'Location','@id':None, 'name':None, 'type':'normal' }}}}}},
    'location_coll':           {'skg':{_prefix:{'':{'locations':    {'':{'@type':'Location','@id':None, 'name':None, 'type':'coll' }}}}}},
    'location_comm':           {'skg':{_prefix:{'':{'locations':    {'':{'@type':'Location','@id':None, 'name':None, 'type':'comm' }}}}}},
    'location_event':          {'skg':{_prefix:{'':{'locations':    {'':{'@type':'Location','@id':None, 'name':None, 'type':'event' }}}}}},
    'language':                {'skg':{_prefix:{'':{'language':                   None }}}},
    'src_journal':             {'skg':{_prefix:{'':{'source_info': { 'src_journal':     None }}}}},
    'src_issue':               {'skg':{_prefix:{'':{'source_info': { 'src_issue':       None }}}}},
    'src_issue_topic':         {'skg':{_prefix:{'':{'source_info': { 'src_issue_topic':  None }}}}},
    'src_volume':              {'skg':{_prefix:{'':{'source_info': { 'src_volume':      None }}}}},
    'src_series':              {'skg':{_prefix:{'':{'source_info': { 'series': {'': {'@type':'Series','@id':None, 'series_name':None }}}}}}},
    'src_pages':               {'skg':{_prefix:{'':{'source_info': { 'from_page':       None, 'to_page':None }}}}},
    'src_collection':          {'skg':{_prefix:{'':{'source_info': { 'collection':      None }}}}},
    'src_conference':          {'skg':{_prefix:{'':{'source_info': { 'conference':      None }}}}},
    'src_conference_number':   {'skg':{_prefix:{'':{'source_info': { 'conference_number': None }}}}},
    'date_conference':         {'skg':{_prefix:{'':{'source_info': { 'conference_date':   None }}}}},
    'src_collection':          {'skg':{_prefix:{'':{'source_info': { 'collection':      None }}}}},
    'rec_title':               {'skg':{_prefix:{'':{'title':None }}}},
    'rec_publisher':           {'skg':{_prefix:{'':{'pub_info':{'publisher':         None }}}}},
    'rec_authors':             {'skg':{_prefix:{'':{'authors':      {'':{'@type':'Person','@id':  None,'id_mention':None,'name':None,'surnames':None,'firstnames':[],'firstinits':[] }}}}}},
    'rec_editors':             {'skg':{_prefix:{'':{'editors':      {'':{'@type':'Person','@id':  None,'id_mention':None,'name':None,'surnames':None,'firstnames':[],'firstinits':[] }}}}}},
    'rec_city':                {'skg':{_prefix:{'':{'pub_info':{'publication_city':         None }}}}},
    'rec_date':                {'skg':{_prefix:{'':{'pub_date': None }}}},
    'rec_edition':             {'skg':{_prefix:{'':{'source_info':{'src_edition':      None }}}}},
    'rec_isbn':                {'skg':{_prefix:{'':{'isbn':None }}}},
    'rec_series':              {'skg':{_prefix:{'':{'source_info':{'series':      None }}}}},
    'reviewed':                {'skg':{_prefix:{'':{'reviewed':                   None }}}},
    'abstract':                {'skg':{_prefix:{'':{'abstract':                   None }}}},
    'toc':                     {'skg':{_prefix:{'':{'toc':                        None }}}},
    'misc':                    {'skg':{_prefix:{'':{'misc':                       None }}}},
    'licence':                 {'skg':{_prefix:{'':{'rights_info':{'licence':                    None }}}}},
    'copyright':               {'skg':{_prefix:{'':{'rights_info':{'copyright':                  None }}}}},
    'sherpa':                  {'skg':{_prefix:{'':{'rights_info':{'sherpa':                     None }}}}},
    'sherpa_de':               {'skg':{_prefix:{'':{'rights_info':{'sherpa_de':                  None }}}}},
    'sherpa_en':               {'skg':{_prefix:{'':{'rights_info':{'sherpa_en':                  None }}}}},
    'doctype':                 {'skg':{_prefix:{'':{'doctypes':{'':{'@type':'Doctype','@id':None, 'name':None }}}}}},
    'doctype_de':              {'skg':{_prefix:{'':{'doctypes':{'':{'name_de':None }}}}}},
    'doctype_en':              {'skg':{_prefix:{'':{'doctypes':{'':{'name':None, 'name_en':None }}}}}},
    'subject_ddc':             {'skg':{_prefix:{'':{'subject_info':{'subject_info_ddc':       {'subjects_ddc':        {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_ddc_en':          {'skg':{_prefix:{'':{'subject_info':{'subject_info_ddc':       {'subjects_ddc_en':     {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_ddc_de':          {'skg':{_prefix:{'':{'subject_info':{'subject_info_ddc':       {'subjects_ddc_de':     {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_thesoz':          {'skg':{_prefix:{'':{'subject_info':{'subject_info_thesoz':    {'subjects_thesoz':     {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_thesoz_en':       {'skg':{_prefix:{'':{'subject_info':{'subject_info_thesoz':    {'subjects_thesoz_en':  {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_thesoz_de':       {'skg':{_prefix:{'':{'subject_info':{'subject_info_thesoz':    {'subjects_thesoz_de':  {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_classhort':       {'skg':{_prefix:{'':{'subject_info':{'subject_info_classhort': {'subjects_classhort':  {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_classoz':         {'skg':{_prefix:{'':{'subject_info':{'subject_info_classoz':   {'subjects_classoz':    {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_classoz_en':      {'skg':{_prefix:{'':{'subject_info':{'subject_info_classoz':   {'subjects_classoz_en': {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_classoz_de':      {'skg':{_prefix:{'':{'subject_info':{'subject_info_classoz':   {'subjects_classoz_de': {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_method':          {'skg':{_prefix:{'':{'subject_info':{'subject_info_method':    {'subjects_method':     {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_method_en':       {'skg':{_prefix:{'':{'subject_info':{'subject_info_method':    {'subjects_method_en':  {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_method_de':       {'skg':{_prefix:{'':{'subject_info':{'subject_info_method':    {'subjects_method_de':  {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_other':           {'skg':{_prefix:{'':{'subject_info':{'subject_info_other':     {'subjects_other':      {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_other_en':        {'skg':{_prefix:{'':{'subject_info':{'subject_info_other':     {'subjects_other_en':   {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'subject_other_de':        {'skg':{_prefix:{'':{'subject_info':{'subject_info_other':     {'subjects_other_de':   {'':{'@type':'Topic','@id':None, 'name': None }}}}}}}},
    'fulltext':                {'skg':{_prefix:{'':{'fulltext':                   None }}}},
};
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-PARSING-----------------------------------------------------------------------------------------------------------------------------------------
def parse(value,field,ID):
    target = copy(_targets[field]);
    if field == 'title':
        target['skg'][_prefix][''][field] = value;
        '''refmap = { ident if ident else doi: {   '@type':    'Publication',
                                                '@id':      ident if ident else doi,
                                                'alt_id':   doi   if ident else None,
                                                'title':    title,
                                                'title_de': title,
                                                'title_en': title,
                                                'score':    score,
                                                'reason':   value,
                                                'to_src':   index } for ident,doi,title,score,relscore,typ,index,date in matcher.match(['study-doi','doi'][_GWS],None,['user-id','id'][_GWS],None,['title-de','title'][_GWS],value,'publication',_GWS)
                                                                                                                       + matcher.match(['study-doi','doi'][_GWS],None,['user-id','id'][_GWS],None,['title-de','title'][_GWS],value,'publication',_GWS,'sowiport') if ident != ID};
        target['skg'][_prefix]['']['duplicates'] = refmap;'''
    elif field == 'author' or field == 'editor' or field == 'contributor':
        mentionID                                               = _prefix+'_'+ID+'_'+norm(value,field).replace(',','').replace('.','').replace(' ','_');
        target['skg'][_prefix][''][field+'s']['']['@type']      = 'Person';
        target['skg'][_prefix][''][field+'s']['']['@id']        = mentionID;
        target['skg'][_prefix][''][field+'s']['']['id_mention'] = mentionID;
        target['skg'][_prefix][''][field+'s']['']['name']       = norm(value,field);
        name_last, names_first, inits_first                     = analyze_name(norm(value,field));
        target['skg'][_prefix][''][field+'s']['']['surname']    = name_last;
        target['skg'][_prefix][''][field+'s']['']['firstnames'] = names_first;
        target['skg'][_prefix][''][field+'s']['']['firstinits'] = inits_first;
    if field == 'rec_authors' or field == 'rec_editors':
        mentionID                                               = _prefix+'_'+ID+'_'+norm(value,field).replace(',','').replace('.','').replace(' ','_');
        target['skg'][_prefix][''][field[4:]]['']['@type']      = 'Person';
        target['skg'][_prefix][''][field[4:]]['']['@id']        = mentionID;
        target['skg'][_prefix][''][field[4:]]['']['id_mention'] = mentionID;
        target['skg'][_prefix][''][field[4:]]['']['name']       = norm(value,field);
        name_last, names_first, inits_first                     = analyze_name(norm(value,field));
        target['skg'][_prefix][''][field[4:]]['']['surname']    = name_last;
        target['skg'][_prefix][''][field[4:]]['']['firstnames'] = names_first;
        target['skg'][_prefix][''][field[4:]]['']['firstinits'] = inits_first;
    elif field == 'src_pages':
        split   = value.split('-'); #TODO: Improve to go for numerical characters
        fro, to = split if len(split)>=2 else (split[0],split[0]) if len(split)==1 else (None,None);
        target['skg'][_prefix]['']['source']['from_page'] = norm(fro,'from_page');
        target['skg'][_prefix]['']['source']['to_page']   = norm(to ,'to_page'  );
    else:
        fill(norm(value,field),target,None);
        if not (isinstance(value,int) or isinstance(value,float)):
            fill(get_terms(value,field),target,Counter());
    return target;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-NORMALIZATIONS----------------------------------------------------------------------------------------------------------------------------------
def norm(value,field): #TODO: Define here for each field how to process the respective value
    if field=='doi' and ':' in value:#value.startswith('doi:'):
        return value.split(':')[-1]; # This will also remove http: etc.
    elif field=='from_gles':
        return bool(value);
    elif field == '@id':
        return value.replace(' ','_');
    return value;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------
rows = cur.execute("SELECT * FROM rows ORDER BY rowid ASC").fetchall();
D    = parse_rows(rows,_initial,_targets,_index2column,_column2index,_id_field,parse);
merge(D,_context);

OUT = open(_outfile,'w');
json.dump(D,OUT,indent=1);
OUT.close();
#-------------------------------------------------------------------------------------------------------------------------------------------------
