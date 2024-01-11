# -*- coding: utf-8 -*-
from EXparser.Segment_F1 import *
from langdetect import detect
from JsonParser import *
from configs import *
from logger import *
import re
import sys
import dataclasses
import json
from bs4 import BeautifulSoup
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import parallel_bulk as bulk
import time

from reference import *

# -------------------------------------------------------------------------------------------------------------------------------------------------
# -GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index = 'ssoar_gold'
_source = 'cermine'
# _workers = int(sys.argv[3]) if len(sys.argv)>3 else 1;
_chunk_size = 5  # how many batch insert in the context of the elasticsearch bulk api
_scroll_size = 15  # how many input docs to retrieve at a time from the index
_max_extract_time = 0.5  # minutes
_max_scroll_tries = 2  # how often to retry when queries failed

_input_indicator = "has_cermine_layout"
_output_field = 'exparser_references_from_' + _source + '_layout'
_output_indicator = 'has_exparser_references_from_' + _source + '_layout'
_output_field_count = 'num_exparser_references_from_' + _source + '_layout'

_body = {
    '_op_type': 'update',
    '_index': _index,
    '_id': None,
    '_source': {
        'doc': {
            _output_field: [],
            _output_indicator: False,
            _output_field_count: None,
        }
    }
}  # this is the body for storing the results in the index via updating of the respective entries

_scr_query = {
    "query": {
        "bool":
        {
            "must": {"term": {_input_indicator: True}},
            "must_not": {"term": {_output_indicator: True}}
        }
    }
}  # which documents to select for further processing


def clean_and_split_text(text, parse_type):
    try:
        clean_text = text.translate({ord(c): " " for c in "–!-@#$%^&*()[]{};:,./<>?\|`~-=_+"}) if not isinstance(text,
                                                                                                                 int) else text
        if parse_type == 'int':
            split_text = [int(s) for s in clean_text.split() if s.isdigit() and int(
                s) <= sys.maxsize]  # check if number is in range of long (-9223372036854775808, 9223372036854775807)
        else:
            split_text = clean_text.split()
        return split_text
    except Exception as e:
        print(e)
        print('\n[!]-----> Could not translate', text, '\n')


def del_none(d):
    for key, value in list(d.items()):
        if not value:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d


def map_exparser_output(exparser_ref_to_map, info):
    soup = BeautifulSoup(exparser_ref_to_map, "lxml")
    titles = soup.find_all('title')
    authors = soup.find_all('author')
    editors = soup.find_all('editor')
    publishers = soup.find_all('publisher')
    dates = soup.find_all('year')
    sources = soup.find_all('source')
    fpage = soup.find('fpage')
    lpage = soup.find('lpage')
    pages = soup.find('page')
    volume = soup.find('volume')
    issue = soup.find('issue')
    others = soup.find_all('other')
    ref = dict()
    raw_ref = re.sub('<.*?>', '', str(exparser_ref_to_map))
    raw_ref = raw_ref.split()
    ref['reference'] = ' '.join(raw_ref).replace(' .', '.').replace(' ,', ',').replace(' -', '-').replace('- ', '-').replace('( ', '(').replace(' )', ')')
    if authors:
        ref['authors'] = []
        for author in authors:
            auth = dict()
            given_names = author.find_all('given-names')
            if given_names:
                auth['initials'] = []
                auth['author_string'] = ''
                for each_fname in given_names:
                    cleaned_split_fnames = clean_and_split_text(each_fname.text, 'str')
                    if cleaned_split_fnames:
                        if len(cleaned_split_fnames[0]) > 1:
                            if 'firstnames' not in auth:
                                auth['firstnames'] = []
                            auth['firstnames'].append(cleaned_split_fnames[0])
                        auth['initials'].append(each_fname.text[0])
                        auth['author_string'] = ' '.join([auth['author_string'], cleaned_split_fnames[0]]) if auth['author_string'] != '' else cleaned_split_fnames[0]

            surname = author.find('surname')
            if surname:
                if 'author_string' not in auth:
                    auth['author_string'] = ''
                # auth['author_type'] = 'Person'
                auth['surname'] = surname.text
            if 'surname' in auth and (
                    auth['surname'] not in auth['author_string']):  # for appending surname at end of string
                auth['author_string'] = ' '.join([auth['author_string'], auth['surname']]) if auth[
                                                                                                  'author_string'] != '' else \
                auth['surname']
                # auth['author_string'] += auth['surname']
            ref['authors'].append(auth)

    if editors:
        ref['editors'] = []
        editor = dict()
        editor['editor_string'] = ''
        for each_editor in editors:
            editor['editor_string'] = ' '.join([editor['editor_string'], each_editor.text]) if editor[
                                                                                                   'editor_string'] != '' else each_editor.text
        ref['editors'].append(editor)

    if publishers:
        ref['publishers'] = []
        publish = dict()
        publish['publisher_string'] = ''
        for each_pub in publishers:
            publish['publisher_string'] = ' '.join([publish['publisher_string'], each_pub.text]) if publish[
                                                                                                        'publisher_string'] != '' else each_pub.text
        ref['publishers'].append(publish)

    if titles:
        ref['title'] = ''
        for each_title in titles:
            ref['title'] = ' '.join([ref['title'], each_title.text]) if ref['title'] != '' else each_title.text

    if sources:
        ref['source'] = ''
        for each_src in sources:
            ref['source'] = ' '.join([ref['source'], each_src.text]) if ref['source'] != '' else each_src.text

    if dates:
        cleaned_split_date = clean_and_split_text(dates[0].text, 'int')
        if cleaned_split_date:
            ref['year'] = cleaned_split_date[0]

    if fpage:
        cleaned_split_fpage = clean_and_split_text(fpage.text, 'int')
        if cleaned_split_fpage:
            ref['start'] = cleaned_split_fpage[0]

    if lpage:
        cleaned_split_lpage = clean_and_split_text(lpage.text, 'int')
        if cleaned_split_lpage:
            ref['end'] = cleaned_split_lpage[0]

    if pages:
        cleaned_split_pages = clean_and_split_text(pages.text, 'int')
        if cleaned_split_pages:
            ref['start'] = cleaned_split_pages[0]
            if len(cleaned_split_pages) > 1:
                ref['end'] = cleaned_split_pages[1]

    if volume:
        cleaned_split_vol = clean_and_split_text(volume.text, 'int')
        if cleaned_split_vol:
            ref['volume'] = cleaned_split_vol[0]

    if issue:
        cleaned_split_issue = clean_and_split_text(issue.text, 'int')
        if cleaned_split_issue:
            ref['issue'] = cleaned_split_issue[0]

    if others:
        if (not lpage or (lpage and lpage.text == '')) and len(others) == 1 and str.isdigit(others[0].text):
            ref['end'] = clean_and_split_text(others[0].text, 'int')[0]
        else:
            ref['place'] = ''
            for each_place in others:
                ref['place'] = ' '.join([ref['place'], each_place.text]) if ref['place'] != '' else each_place.text

    return ref


def extract_segment_exparser(layout_file_string: str):
    segmented_references = []

    global lng
    try:
        lng = detect(layout_file_string)
    except:
        # print("Cannot extract language from " + layout_files_path)
        print("Cannot extract language for layout string")
        lng = ""

    # todo: pass one model: rf as we have one model now for extraction for de and en
    txt, valid, _, ref_prob0 = ref_ext(layout_file_string, lng, idxx, rf, rf)
    refs = segment(txt, ref_prob0, valid)
    reslt, refstr, retex = sg_ref(txt, refs, 1)

    # result: segmented references # refstr: refstr references # retex: bibtex
    log('Number of references: ' + str(len(refstr)))
    # print('Number of references: ' + str(len(refstr)))

    for item, ref in zip(reslt, txt):
        segmented_references.append(map_exparser_output(item, ref))

    # return [(item, ref) for item, ref in zip(reslt, txt)], True
    if len(segmented_references) >= 2:
        return segmented_references[1:], True  # It seems that the first reference is not a reference but the entire fulltext
    return segmented_references, True #return segmented_references, True


def update_exparser_references():
    client = ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
    page = client.search(index=_index, scroll=str(int(_max_extract_time * _scroll_size)) + 'm', size=_scroll_size,
                         body=_scr_query)
    sid = page['_scroll_id']
    returned = len(page['hits']['hits'])
    page_num = 0
    while returned > 0:
        for doc in page['hits']['hits']:
            try:
                print('updating', doc['_id'])
                # ---------------------------------------------------------------------------------------------------------------------------------------
                cermine_layout = doc['_source']['cermine_layout'] if '_source' in doc and 'cermine_layout' in doc['_source'] else None
                extracted_exparser_references, success = extract_segment_exparser(cermine_layout) if cermine_layout and isinstance(cermine_layout, str) and len(cermine_layout) <= 2000000 else (([{'error_message': '[TOO_MANY_TOKENS] The document has ' + str(len(cermine_layout)) + ' tokens, but the limit is 2000000'}], True) if len(cermine_layout) > 2000000 else ([], False))

                body = copy(_body)
                body['_id'] = doc['_id']
                body['_source']['doc'][_output_field] = extracted_exparser_references if success else []
                body['_source']['doc'][_output_indicator] = success
                body['_source']['doc'][_output_field_count] = len(extracted_exparser_references) if success else 0
            except Exception as e:
                print(e)
                print('[!]-----> Some problem occurred while processing document', doc['_id'], '\n')
                body = copy(_body)
                body['_id'] = doc['_id']  # use same id in modified document to update old one
                body['_source']['doc'][_output_field] = []  # extend old content
                body['_source']['doc'][_output_indicator] = False
                body['_source']['doc'][_output_field_count] = 0
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
                print('\n[!]-----> Some problem occurred while scrolling. Sleeping for 3s and retrying...\n')
                returned = 0
                scroll_tries += 1
                time.sleep(3)
                continue
            break
        # break
    client.clear_scroll(scroll_id=sid)


_client = ES(['svko-outcite.gesis.intra'], scheme='http', port=9200, timeout=60)
# for body in update_exparser_references():  # This is for testing what happens # TODO: comment out when applying
#     print(body['_id'])


# TODO: use when applying the tested procedure to actually update the index
i = 0
for success, info in bulk(_client, update_exparser_references(), chunk_size=_chunk_size):
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
