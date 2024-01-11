import sys
import json
from bs4 import BeautifulSoup
import M_utils as ut

_infile  = sys.argv[1];
_outfile = sys.argv[2];


def extract_cioffi_references(xml):
    refobjs = []
    soup    = BeautifulSoup(xml, "xml")
    root    = soup.find('TEI')
    text    = root.findChildren('text', recursive='false') if root else []
    for children in text:
        refs = children.find('listBibl').find_all('biblStruct')
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
                        if title.get('level') == 'a' or len(titles) == 1:
                            refobj['title'] = title.text
                        elif title.get('level') == 'j':
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
                refobjs.append(refobj)
    return refobjs, True #TODO: Any conditions within this function for not success?


IN                  = open(_infile);
xml                 = IN.read(); IN.close();
refobjects, success = extract_cioffi_references(xml);

OUT = open(_outfile,'w');
OUT.write('\n'.join([json.dumps(obj) for obj in refobjects if len(obj)>0]));
OUT.close();
