# -*- coding: utf-8 -*-
#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
from copy import deepcopy as copy
from collections import Counter
import re
import sqlite3
from elasticsearch import Elasticsearch as ES
import typing
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_empty          = set([' - ','-','',]);
_true           = set(['true','True','yes','Yes','ja','Ja','ja.','Ja.','T','correct','Correct']);
_roman_numerals = Counter({'I':1,'V':5,'X':10,'L':50,'C':100,'D':500,'M':1000,'i':1,'v':5,'x':10,'l':50,'c':100,'d':500,'m':1000});
_headers        = ['target','origin','map_type','var_type','bound','splits'];
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-PART-1--------------------------------------------------------------------------------------------------------------------------------
def from_roman(num):
    result = 0;
    for i,c in enumerate(num):
        if (i+1) == len(num) or _roman_numerals[c] >= _roman_numerals[num[i+1]]:
            result += _roman_numerals[c];
        else:
            result -= _roman_numerals[c];
    return result;

def printer():
    keys = set();
    for item in items:
        for field in item:
            if not field in keys:
                keys.add(field); print(field); print(item[field], '\n---------------------------------');

def cast(val,typ):
    if val in _empty:
        return None;
    if typ == 'int':
        try:
            return int(val);
        except:
            number = from_roman(val);
            if number != 0:
                return number;
            print('Failed to convert', val, 'to int...');
            return None;
    if typ == 'float':
        try:
            return float(typ);
        except:
            print('Failed to convert', val, 'to float...');
            return None;
    if typ == 'bool':
        try:
            return True if val in _true else False;
        except:
            print('Failed to convert', val, 'to bool...');
            return None;
    if typ == 'str':
        return val;

def get_mapping(MAP):
    MAP          = open(MAP);
    mapping      = [[el.rstrip().strip() for el in row.split(' , ')] for row in MAP];
    index2target = list(set([row[0] for row in mapping]));
    target2index = {index2target[i]:i for i in range(len(index2target))};
    origin_map   = {row[1]:{_headers[i]:row[i] for i in range(len(row))} for row in mapping};
    MAP.close();
    return mapping, index2target, target2index, origin_map;

def get_paths(edges,root,path):
    for child in edges[root]:
        if child in edges:
            for path_ in get_paths(edges,child,path+[child]):
                yield path_;
        else:
            yield tuple(path+[child]);

def traverse_(d,up_bound_val,origin_map):  # TODO: This requires less memory because it uses generators, but somehow the database gets too large
    for key in d:
        if key in origin_map:
            bound      = origin_map[key]['bound'];
            bound_val  = up_bound_val if not bound in d else d[bound][0] if origin_map[bound]['map_type'][0]=='1' and isinstance(d[bound],list) else d[bound];
            key_vals   = d[key  ]     if isinstance(d[key],list) else [val.strip() for val in re.sub(origin_map[key]['splits'],'#*#',d[key]).split('#*#')] if origin_map[key]['map_type']=='1n' else [key] if isinstance(d[key],dict) else [d[key]];
            key_type   = origin_map[key]['var_type'];
            bound_type = origin_map[bound]['var_type'];
            for key_val in key_vals:
                if key_type=='dict':#isinstance(key_val,dict):
                    yield ((origin_map[bound]['target'],bound_val,),(origin_map[key]['target'],cast(bound_val,bound_type),),);
                    for pair in traverse(key_val,bound_val,origin_map):
                        yield pair;
                else:
                    yield ((origin_map[bound]['target'],bound_val,),(origin_map[key]['target'],cast(key_val,key_type),),);

def traverse(d,up_bound_val,pairs,origin_map): # This is the main function. It should traverse the dict and use the bound if exists in the current dict's keys else use the bound passed down
    for key in d:
        if not isinstance(key,typing.Hashable):
            print('ERROR: not hashable -->',key);
        if key in origin_map:
            bound      = origin_map[key]['bound'];
            bound_val  = up_bound_val if not bound in d else d[bound][0] if origin_map[bound]['map_type'][0]=='1' and isinstance(d[bound],list) else d[bound];
            key_vals   = d[key  ]     if isinstance(d[key],list) else [val.strip() for val in re.sub(origin_map[key]['splits'],'#*#',d[key]).split('#*#')] if origin_map[key]['map_type']=='1n' else [key] if isinstance(d[key],dict) else [d[key]];
            key_type   = origin_map[key]['var_type'];
            bound_type = origin_map[bound]['var_type'];
            for key_val in key_vals:
                if key_type=='dict':#isinstance(key_val,dict):
                    pairs.add(((origin_map[bound]['target'],bound_val,),(origin_map[key]['target'],cast(bound_val,bound_type),),));
                    traverse(key_val,bound_val,pairs,origin_map);
                else:
                    pairs.add(((origin_map[bound]['target'],bound_val,),(origin_map[key]['target'],cast(key_val,key_type),),));

def combine(field_primary,fields_secondary,field_id,field_item_id,field_parent,items):
    for j in range(len(items)):
        if field_primary in items[j]:
            for i in range(len(items[j][field_primary])):
                if field_parent in items[j]:
                    items[j][field_parent].append({field_primary:items[j][field_primary][i],field_id:field_parent+'_'+items[j][field_item_id]+'_'+str(i)});
                else:
                    items[j][field_parent] = [{field_primary:items[j][field_primary][i],field_id:field_parent+'_'+items[j][field_item_id]+'_'+str(i)}];
            del items[j][field_primary];
            for field_secondary in fields_secondary:
                for i in range(len(items[j][field_secondary])): # Assuming that there are always both English and German values
                    items[j][field_parent][i][field_secondary] = items[j][field_secondary][i];
                del items[j][field_secondary];
    return items;

def make_unique(items,parent_prefixes): #TODO: This only works for dicts that are directly under the root
    for item in items:
        item_ = copy(item);
        for parent, prefix in parent_prefixes:
            if parent in item_:
                if not isinstance(item_[parent],list):
                    item_[parent] = [item_[parent]];
                for j in range(len(item_[parent])):
                    keys = list(item_[parent][j].keys());
                    for key in keys:
                        item_[parent][j][prefix+'-'+key] = item_[parent][j][key];
                    for key in keys:
                        del item_[parent][j][key];
        yield item_;

def get_items_exdat(batch,maxlen,client,index,body):
    print('Start scrolling...');
    results   = client.search(index=index,scroll='2m',size=batch,body=body);
    sid       = results['_scroll_id'];
    length    = len(results['hits']['hits']);
    processed = 0;
    while length > 0:
        processed += length;
        print(index+': Number of items scrolled over:', processed);
        #--------------------------------------------------------------
        for result in results['hits']['hits']:
            #if 'q-id' in result['_source']:
            yield result['_source'];
        #--------------------------------------------------------------
        results = client.scroll(scroll_id=sid, scroll='2m');
        sid     = results['_scroll_id'];
        length  = len(results['hits']['hits']);
        if processed > maxlen: break;

def get_items_gws(batch,maxlen,index_source,typ,query=None):
    client    = ES(['http://search.gesis.org:9200'],timeout=60); #{"bool": {"must":[{"term":{"index_source": index_source }},{"term":{"type":typ}}]}}
    query     = query if query else {"bool": {"must":[{"match":{"index_source":index_source}},{"term":{"type":typ}}]}} if index_source!=None else {"term":{"type":typ}};
    page      = client.search(index='gesis',scroll='2m',size=batch,query=query);
    sid       = page['_scroll_id'];
    returned  = len(page['hits']['hits']);print(query,returned)
    page_num  = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            yield doc['_source'];
        scroll_tries = 0;
        while scroll_tries < 2:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(1*batch))+'m');
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

def convert(items,prefix,origin_map,r_orgn,r_trgt,target2index):
    pairs = get_pairs(items,prefix,origin_map);
    edges = get_edges(pairs);
    #for edge in edges:
    #    print(edge);
    #    input('Enter to continue...');
    paths = make_paths(edges,items,r_orgn,r_trgt);
    rows  = get_rows(paths,target2index);
    return rows;

def get_pairs_(items,prefix,origin_map): # TODO: This requires less memory because it uses generators, but somehow the database gets too large
    processed = 0;
    for item in items:
        processed += 1;
        if processed % 10000 == 0: print(processed,end='\r');
        for pair in traverse(item,prefix,origin_map):
            yield pair;

def get_pairs(items,prefix,origin_map):
    processed = 0;
    pairs     = set([]);
    for item in items:
        processed += 1;
        if processed % 10000 == 0: print((100.*processed)/len(items), '%');
        if not isinstance(item,dict):
            print('ERROR: item is not a dict -->',item);
        traverse(item,prefix,pairs,origin_map);
    return pairs;

def get_edges(pairs):
    processed = 0;
    edges     = dict();
    for bound, key in pairs:
        processed += 1;
        if processed % 10000 == 0: print(processed,end='\r');
        if bound in edges:
            edges[bound].append(key);
        else:
            edges[bound] = [key];
    return edges;

def make_paths(edges,items,r_orgn,r_trgt):
    roots = set([(r_trgt,item[r_orgn][0],) if isinstance(item[r_orgn],list) else (r_trgt,item[r_orgn],) for item in items]);
    for root in roots:
        for path in get_paths(edges,root,[root]):
            yield path;

def get_rows(paths,target2index):
    row = [None for i in range(len(target2index))];
    for path in paths:
        for key,value in path:
            if row[target2index[key]] not in [None,value]:
                yield row;
                row = [None for i in range(len(target2index))];
                break;
        for key,value in path:
            row[target2index[key]] = value;
    yield row;

def insert_rows(rows,index2target,OUT):
    con = sqlite3.connect(OUT);
    cur = con.cursor();
    cur.execute("DROP   TABLE IF EXISTS rows");
    cur.execute("CREATE TABLE rows(rowid INTEGER PRIMARY KEY AUTOINCREMENT, "+', '.join(index2target)+")");
    cur.executemany("INSERT INTO rows("+', '.join(index2target)+") VALUES("+','.join(['?' for i in range(len(index2target))])+")",rows);
    con.commit(); con.close();
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-PART-2--------------------------------------------------------------------------------------------------------------------------------
def fill(value,D,empty):
    for key in D:
        if type(D[key])==dict:
            fill(value,D[key],empty);
        elif D[key] == empty:
            D[key] = value if key != '@id' else value.replace(' ','_');
        elif D[key] == [empty]:
            D[key] = [value];

def clear_ids(d):
    for key in list(d.keys()):
        pointer = d[key];
        if len(key)==0:
            if '@id' in d['']:
                ID = d['']['@id'];
                #print ID;
                if ID == None:
                    #print 'Has @id but None. Removing', d[''];
                    del d[''];
                elif ID in d:
                    merge(d[ID],d.pop(''));
                else:
                    d[ID] = d.pop('');
            else:
                #print 'No @id. Removing', d[''];
                del d[''];
        if isinstance(pointer, dict):
            clear_ids(pointer);

def clear_lists(d): # This can remove the dict structure for @id entries and replace it by a list of dicts
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

def get_dict_with_key(key, obj):
    if hasattr(obj,'iteritems'):
        for k, v in obj.items():
            if k == key:
                yield obj;#yield v;
            if isinstance(v, dict):
                for obj_ in get_dict_with_key(key,v):
                    yield obj_;

def parse_rows(rows,initial,targets,index2column,column2index,id_field,parse):
    D = dict();
    for i in range(len(rows)):
        if i % 10000 == 0: print(i);
        d = copy(initial);
        for j in range(len(rows[i])):
            value = rows[i][j];
            field = index2column[j];
            ID    = rows[i][column2index[id_field]];
            if value != None and field in targets:
                merge(d,parse(value,field,ID));
        clear_ids(d);
        merge(D,d);
    return D;

def merge(d, u):
    for k, v in u.items():
        if v == None:                                   # discard None values
            continue;
        elif (not k in d) or d[k] == None:              # new key or old value was None
            d[k] = v;
        elif isinstance(v,Counter):                     # Counters are added
            d[k] = d[k] + v;
        elif isinstance(v,dict) and v != {}:            # non-Counter dicts are merged
            d[k] = merge(d.get(k,{}),v);
        elif isinstance(v,set):                         # set are joined
            d[k] = d[k] | v;
        elif isinstance(v,list):                        # list are concatenated
            d[k] = d[k] + v;
        elif isinstance(v,int) or isinstance(v,float):  # int and float are added
            d[k] = d[k] + v;
        elif v != dict():                               # anything else is replaced
            d[k] = v;
    return d;

def get_terms(value,field): #TODO: Improve this function
    return Counter() if value==None else Counter(value.split());

def analyze_name(value,primary_split=', ',secondary_split=' '): #TODO: Improve this function
    elements              = value.split(primary_split);
    name_last             = elements[0];
    firstnamestring       = elements[1] if len(elements) >= 2 else None;
    if firstnamestring == None:
        return name_last, [], [];
    firstnamestring_parts = firstnamestring.split(secondary_split);
    names_first           = [];
    inits_first           = [];
    for part in firstnamestring_parts:
        part_ = part.replace('.','').strip();
        if len(part_) == 1:
            names_first.append(None);
            inits_first.append(part_);
        elif len(part_) > 1:
            names_first.append(part_);
            inits_first.append(part_[0]);
    return name_last, names_first, inits_first;
#-------------------------------------------------------------------------------------------------------------------------------------------------
