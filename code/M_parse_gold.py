import sys
import re
import xmltodict
from geopy.geocoders import Nominatim
from copy import deepcopy as copy
from time import sleep
import json
import roman

_infile  = sys.argv[1];
_outfile = sys.argv[2];

_geolocator    = Nominatim(user_agent="OUTCITE Project");
_max_geo_tries = 3;

_multifields  = ['author','editor','publisher','given-names','initials','other']
_singlefields = ['year','volume','fpage','lpage','issue','title','author-string','surnames']

MONTHS = {'january':1,'february':2,'march':3,'april':4,'may':5,'june':6,'july':7,'august':8,'september':9,'oktober':10,'november':11,'december':12,
          'januar': 1,'februar': 2,'märz': 3,          'mai':5,'juni':6,'juli':7,                                                    'dezember':12};

CONTENT = re.compile(r'>[^<>]*<');
GARBAGE = re.compile(r'>[^<>]*$');
TAG     = re.compile(r'<[^<>]*>');
SEP     = re.compile(r'\s|-|\.\s|\.-');
LOCSEP  = re.compile(r'\s*\/\s*')
YEARSEP = re.compile(r'\s*\/|-\s*')

_publisher = { "publisher_type":   None,
               "publisher_string": None };

_editor    = { "editor_type":   None,
               "editor_string": None,
               "surnames":      None,
               "initials":      [],
               "firstnames":    [] };

_author    = { "author_type":   None,
               "author_string": None,
               "surnames":      None,
               "initials":      [],
               "firstnames":    [] };

IN        = open(_infile);
lines     = [line.rstrip() for line in IN.readlines()];
IN.close();

def remove_nones(value):
    if isinstance(value, list):
        return [remove_nones(x) for x in value if x is not None];
    elif isinstance(value, dict):
        return {key: remove_nones(val) for key,val in value.items() if val is not None};
    else:
        return value;

def get_integer(string):
    integer = None;
    try:
        integer = int(string);
    except:
        try:
            integer = roman.fromRoman(string);
        except:
            try:
                integer = MONTHS[string.lower()];
            except:
                print('Could not get integer of',string);
    return integer;

def parse_geocite(d):
    d_ = dict();
    for key in d:
        if key == 'title':
            d_['title'] = d[key];
        if key == 'author':
            authors = [];
            for author in d[key]:
                authors.append(copy(_author));
                for key in author:
                    if   key == 'surname':
                        authors[-1]['surnames'] = author[key][0]+author[key][1:].lower();
                    elif key == 'given-names':
                        for given in author[key]:
                            if len(given) == 1:
                                authors[-1]['initials'].append(given);
                            else:
                                if len(given) >= 1:
                                    authors[-1]['initials'].append(given[0]);
                                if len(given) >= 2:
                                    authors[-1]['firstnames'].append(given);
                        authors[-1]['author_string'] = ' '.join(author[key]+[authors[-1]['surnames']]) if author[key] and authors[-1]['surnames'] else authors[-1]['surnames'] if not author[key] else author[key];
            d_['authors'] = authors;
        elif key == 'editor':
            editors = [];
            for editor in d[key]:
                editors.append(copy(_editor));
                editors[-1]['editor_string'] = editor;
            d_['editors'] = editors;
        elif key == 'publisher':
            publishers = [];
            for publisher in d[key]:
                publishers.append(copy(_publisher));
                publishers[-1]['publisher_string'] = publisher;
            d_['publishers'] = publishers;
        elif key == 'fpage':
            d_['start'] = get_integer(d[key]);
        elif key == 'lpage':
            d_['end'] = get_integer(d[key]);
        elif key == 'year':
            d_['year'] = get_integer(d[key]);
        elif key == 'volume':
            d[key] = YEARSEP.split(d[key])[0] if d[key] else None;
            d_['volume'] = get_integer(d[key]);
        elif key == 'issue':
            d[key] = YEARSEP.split(d[key])[0] if d[key] else None;
            d_['issue'] = get_integer(d[key]);
        elif key == 'other':
            others = [el for other in d[key] for el in LOCSEP.split(other)];
            places = locator(others);
            d_['place'] = places;
        elif key == 'source':
            d_['source'] = d[key];
        elif key == 'reference':
            d_['reference'] = d[key];
    return remove_nones(d_);

def parse_ssoar_gold(d):
    d_ = dict();
    for key in d:
        if key == 'author':
            authors = [];
            for author in d[key]:
                authors.append(copy(_author));
                for key in author:
                    if key == 'surname':
                        try:
                            authors[-1]['surnames'] = author[key][0]+author[key][1:].lower();
                        except Exception as e:
                            print(e); print(author[key]);
                    elif key == 'given-names':
                        for given in author[key]:
                            if not given:
                                continue;
                            if len(given) == 1:
                                authors[-1]['initials'].append(given);
                            else:
                                if len(given) >= 1:
                                    authors[-1]['initials'].append(given[0]);
                                if len(given) >= 2:
                                    authors[-1]['firstnames'].append(given);
                name_info = [];
                if 'initials' in authors[-1] and isinstance(authors[-1]['initials'],list):
                    for i in range(len(authors[-1]['initials'])):
                        name_info.append(authors[-1]['firstnames'][i] if 'firstnames' in authors[-1] and isinstance(authors[-1]['firstnames'],list) and i < len(authors[-1]['firstnames']) else authors[-1]['initials'][i]);
                if 'surnames' in authors[-1] and isinstance(authors[-1]['surnames'],str):
                    name_info.append(authors[-1]['surnames']);
                authors[-1]['author_string'] = ' '.join(name_info);
                #authors[-1]['author_string'] = ' '.join(authors[-1]['firstnames']+[authors[-1]['surnames']]) if 'firstnames' in authors[-1] and authors[-1]['firstnames'] and 'surnames' in authors[-1] and authors[-1]['surnames'] else ' '.join(authors[-1]['initials']+[authors[-1]['surnames']]) if 'initials' in authors[-1] and authors[-1]['initials'] and 'surnames' in authors[-1] and authors[-1]['surnames'] else authors[-1]['surnames'] if 'surnames' in authors[-1] and authors[-1]['surnames'] else None;
            d_['authors'] = authors;
        elif key == 'editor':
            editors = [];
            for editor in d[key]:
                print('EDITOR:',editor)
                editors.append(copy(_editor));
                editors[-1]['editor_string'] = editor;
            print('EDITORS:',editors);
            d_['editors'] = editors;
        elif key == 'publisher':
            publishers = [];
            for publisher in d[key]:
                print('PUBLISHER:',publisher)
                publishers.append(copy(_publisher));
                publishers[-1]['publisher_string'] = d[key];
            print('PUBLISHERS:',publishers);
            d_['publishers'] = publishers;
        elif key == 'fpage':
            d_['start'] = get_integer(d[key]);
        elif key == 'lpage':
            d_['end'] = get_integer(d[key]);
        elif key == 'year':
            d_['year'] = get_integer(d[key]);
        elif key == 'volume':
            d[key] = YEARSEP.split(d[key])[0] if d[key] else None;
            d_['volume'] = get_integer(d[key]);
        elif key == 'issue':
            d[key] = YEARSEP.split(d[key])[0] if d[key] else None;
            d_['issue'] = get_integer(d[key]);
        elif key == 'other':
            others = [el for other in d[key] for el in LOCSEP.split(other)]; #TODO: In one case other is not a string
            print('OTHERS:',others);
            places = locator(others);
            d_['place'] = places;
        elif key == 'source':
            d_['source'] = d[key];
        elif key == 'reference':
            d_['reference'] = d[key];
        elif key == 'title':
            if isinstance(d[key],str):
                d_['title'] = d[key];
            else:
                d_['title'] = d[key][key];
                print('********',d[key]);
    return remove_nones(d_);

def locator(others):
    return None;
    places = [];
    for other in others:
        print('LOOKING UP:', other)
        place = None;
        tries = 0;
        while tries < _max_geo_tries:
            try:
                place = _geolocator.geocode(other);
            except Exception as e:
                #print(e);
                print('Could not access geolookup. Retrying...');
                tries += 1;
                sleep(1);
        if place != None:
            print('RETURNED:',place);
            places.append(other);
    if len(places) > 1:
        print('WARNING: Multiple places:',places);
    return places;

def clean(d):
    if isinstance(d,list):
        d_ = [];
        for el in d:
            d_.append(clean(el));
        return d_;
    if not isinstance(d,dict):
        return d;
    if len(d) == 1 and list(d.keys())[0] == 'C':
        return d['C'];
    d_ = dict();
    for key in d:
        if key != 'C':
            if key in _multifields and not isinstance(d[key],list):
                d_[key] = clean([d[key]]);
            elif key in _singlefields and isinstance(d[key],list):
                d_[key] = clean(d[key][0]);
            else:
                d_[key] = clean(d[key]);            
            if key=='given-names' and len(d_[key])==1 and d_[key][0]:
                d_[key] = SEP.split(d_[key][0].strip().strip('.').strip());
    return d_;


refobjects = [];
for line in lines:
    string = TAG.sub('',line).rstrip();
    line_o = line;
    line   = GARBAGE.sub('>',line);
    line_  = '<ref>';
    prev   = 0;
    for match in CONTENT.finditer(line):
        start, end = match.span();
        text       = match.group(0)[1:-1].rstrip().strip();
        if len(text) == 0:
            continue;
        #line_ += line[prev:start] + '><C>' + text + '</C><';
        line_ += line[prev:start] + '><![CDATA[' + text + ']]><';
        prev   = end;
    line_ += line[prev:];
    line_ += '</ref>';
    try:
        dictionary = xmltodict.parse(line_);
    except Exception as e:
        print(e);
        print('---------------------------------------------------------------\n'+line_);
        print('---------------------------------------------------------------\n'+line_o);
        print('ERROR: Failed to parse line as XML\n---------------------------------------------------------------');
        continue;
    refobjects.append(dictionary);
    if isinstance(refobjects[-1]['ref'],dict):
        refobjects[-1]['ref']['reference'] = string;


for i in range(len(refobjects)):
    refobjects[i] = clean(refobjects[i]['ref']);

objs = [parse_ssoar_gold(refobject) for refobject in refobjects]; #TODO: See if this works otherwise make a case distinction between geocite and ssoar_gold

OUT = open(_outfile,'w');
OUT.write('\n'.join([json.dumps(obj) for obj in objs if len(obj)>0]));
OUT.close();
