import json
from collections import Counter
from copy import deepcopy as copy
from difflib import SequenceMatcher as SM
import numpy as np
from scipy.optimize import linear_sum_assignment as LSA
from elasticsearch.helpers import streaming_bulk as bulk
from elasticsearch import Elasticsearch as ES
import sys
import re

_threshold = 0.25;

_index            = sys.argv[1];
_chunk_size       = 10;
_scroll_size      = 10;
_max_extract_time = 5; #minutes
_max_scroll_tries = 2;

_recheck = True; #TODO: False does not really work as it seems

_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',
                #'anystyle_references_from_gold_fulltext',
                #'anystyle_references_from_gold_refstrings',
                'anystyle_references_from_pdftotext_fulltext',
                'cermine_references_from_cermine_xml',
                'cermine_references_from_grobid_refstrings',
                #'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml',
                'exparser_references_from_cermine_layout',
                #'merged_references'
           ];
#_refobjs = ['anystyle_references_from_cermine_fulltext'];
_refobj  = None;

_body = None;

_scr_query = {'term':{'has_gold_refobjects': True}} if _recheck else {'bool':{'must':[{'term':{'has_gold_refobjects': True}}],'must_not':[{'term':{'has_result': True}}]}};

GARBAGE = re.compile(r'\W')#re.compile(r'[\x00-\x1f\x7f-\x9f]|(-\s+)');
NAMESEP = re.compile(r'\W');
YEAR    = re.compile(r'1[5-9][0-9]{2}|20(0[0-9]|1[0-9]|2[0-3])'); #1500--2023


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

def show_results(refobj,client):
    body                 = {'query':{'term':{'has_results_'+refobj: True}},'_source':['results_'+refobj]};
    page                 = client.search(index=_index,scroll=str(_max_extract_time*_scroll_size)+'m',size=_scroll_size,body=body);
    sid                  = page['_scroll_id'];
    returned             = len(page['hits']['hits']);
    page_num             = 0;
    TP_str, P_str, T_str = 0,0,0;
    TP_obj, P_obj, T_obj = 0,0,0;
    Prec_str, Rec_str    = 0,0;
    Prec_obj, Rec_obj    = 0,0;
    keywise              = dict();
    number               = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            number   += 1;
            TP_str   += doc['_source']['results_'+refobj]['keywise']['_reference']['TP'];#['refobj']
            P_str    += doc['_source']['results_'+refobj]['keywise']['_reference']['P'];#['refobj']
            T_str    += doc['_source']['results_'+refobj]['keywise']['_reference']['T'];#['refobj']
            Prec_str += doc['_source']['results_'+refobj]['keywise']['_reference']['precision'];#['refobj']
            Rec_str  += doc['_source']['results_'+refobj]['keywise']['_reference']['recall'];#['refobj']
            TP_obj   += doc['_source']['results_'+refobj]['TP'];#['refobj']
            P_obj    += doc['_source']['results_'+refobj]['P'];#['refobj']
            T_obj    += doc['_source']['results_'+refobj]['T'];#['refobj']
            Prec_obj += doc['_source']['results_'+refobj]['precision'];#['refobj']
            Rec_obj  += doc['_source']['results_'+refobj]['recall'];#['refobj']
            keywise   = merge(keywise,doc['_source']['results_'+refobj]['keywise']);#['refobj']
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(_max_extract_time*_scroll_size)+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    if P_str == 0 or P_obj == 0:
        print('No result possible as P=0');
    else:
        print('REFSTR:','[ALL]','PREC:',int(100*TP_str/P_str),   'REC:',int(100*TP_str/T_str),  'F1:',int(100*2*(TP_str/P_str)*(TP_str/T_str)/(TP_str/P_str+TP_str/T_str)),'TP:',TP_str,'P:',P_str,'T:',T_str);
        print('       ','[AVG]','PREC:',int(100*Prec_str/number),'REC:',int(100*Rec_str/number),'F1:',int(100*2*(Prec_str/number)*(Rec_str/number)/(Prec_str/number+Rec_str/number)));
        print('REFOBJ:','[ALL]','PREC:',int(100*TP_obj/P_obj),   'REC:',int(100*TP_obj/T_obj),  'F1:',int(100*2*(TP_obj/P_obj)*(TP_obj/T_obj)/(TP_obj/P_obj+TP_obj/T_obj)),'TP:',TP_obj,'P:',P_obj,'T:',T_obj);
        print('       ','[AVG]','PREC:',int(100*Prec_obj/number),'REC:',int(100*Rec_obj/number),'F1:',int(100*2*(Prec_obj/number)*(Rec_obj/number)/(Prec_obj/number+Rec_obj/number)));
        for key in keywise:
            print('\n       ',key+' [ALL]:','PREC:',int(100*keywise[key]['TP']/keywise[key]['P']) if keywise[key]['P']>0 else 0,   'REC:',int(100*keywise[key]['TP']/keywise[key]['T']) if keywise[key]['T']>0 else 0,  'F1:',int(100*2*(keywise[key]['TP']/keywise[key]['P'])*(keywise[key]['TP']/keywise[key]['T'])/(keywise[key]['TP']/keywise[key]['P']+keywise[key]['TP']/keywise[key]['T'])) if keywise[key]['P']>0 and keywise[key]['T']>0 else 0,'TP:',keywise[key]['TP'],'P:',keywise[key]['P'],'T:',keywise[key]['T']);
            numerator   = int(100*2*(keywise[key]['precision']/number)*(keywise[key]['recall']/number));
            denominator = keywise[key]['precision']/number+keywise[key]['recall']/number;
            print('       ',''.join([' ' for letter in key])+' [AVG]:','PREC:',int(100*keywise[key]['precision']/number),'REC:',int(100*keywise[key]['recall']/number),'F1:',numerator/denominator if denominator>0 else 0);
        print('------------------------------------------------------------');

def distance(a,b):
    a,b        = a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    return 1-(overlap / max([len(a),len(b)]));

def distance_2(a,b):
    a,b      = a.lower(), b.lower();
    s        = SM(None,a,b);
    overlap  = sum([block.size for block in s.get_matching_blocks()]);
    dist     = max([len(a),len(b)]) - overlap;
    return dist;

def distance_3(a,b):
    a,b        = '_'+re.sub(GARBAGE,'',a.lower()),'_'+re.sub(GARBAGE,'',b.lower());#a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size**1 for block in s.get_matching_blocks() if block.size>=2]);
    dist       = min([len(a),len(b)])**1-overlap;
    return dist;

def flatten(d, parent_key='', sep='_'):
    items = [];
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k;
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items());
        else:
            items.append((new_key, v));
    return dict(items);

def pairfy(d, parent_key='', sep='_'): # To be applied after flatten!
    for key in d:
        if isinstance(d[key],list):
            for el in d[key]:
                if isinstance(el,dict):
                    for a,b in pairfy(el,key,sep):
                        yield (a,str(b),);
                else:
                    yield (parent_key+sep+key,str(el),);
        else:
            yield (parent_key+sep+key,str(d[key]),);

def dictfy(pairs):
    d = dict();
    for attr,val in pairs:
        if not attr in d:
            d[attr] = [];
        d[attr].append(val);
    return d;

def assign(A,B): # Two lists of strings
    #print(A); print(B); print('---------------------------------------------------------');
    M          = np.array([[distance_3(a,b) if isinstance(a,str) and isinstance(b,str) else a!=b for b in B] for a in A]);
    rows, cols = LSA(M);
    mapping    = [pair for pair in zip(rows,cols)];
    costs      = [M[assignment] for assignment in mapping];
    return mapping,costs;

def similar_enough(a,b,cost,threshold):
    if isinstance(a,str) and isinstance(b,str):
        if YEAR.fullmatch(a) and YEAR.fullmatch(b):
            y1, y2 = int(a), int(b);
            return abs(y1-y2) <= 1; # A one year difference between years is accepted
        return cost / min([len(a),len(b)])**1 < threshold;#max and not **1
    return a == b;

def compare_refstrings(P_strings,T_strings,threshold): # Two lists of strings
    mapping,costs = assign(P_strings,T_strings);
    pairs         = [(P_strings[i],T_strings[j],) for i,j in mapping];
    matches       = [(P_strings[mapping[i][0]],T_strings[mapping[i][1]],) for i in range(len(mapping)) if     similar_enough(P_strings[mapping[i][0]],T_strings[mapping[i][1]],costs[i],threshold)];
    mismatches    = [(P_strings[mapping[i][0]],T_strings[mapping[i][1]],) for i in range(len(mapping)) if not similar_enough(P_strings[mapping[i][0]],T_strings[mapping[i][1]],costs[i],threshold)];
    precision     = len(matches) / len(P_strings);
    recall        = len(matches) / len(T_strings);
    return precision, recall, len(matches), len(P_strings), len(T_strings), matches, mismatches, mapping, costs;

def compare_refobject(P_dict,T_dict,threshold):                       # Two dicts that have already been matched based on refstring attribute
    P_pairs     = pairfy(flatten(P_dict));                            # All attribute-value pairs from the output dict
    T_pairs     = pairfy(flatten(T_dict));                            # All attribute-value pairs from the gold   dict
    P_pair_dict = dictfy(P_pairs);                                    # Output values grouped by attributes in a dict
    T_pair_dict = dictfy(T_pairs);                                    # Gold   values grouped by attributes in a dict
    P_keys      = set(P_pair_dict.keys());                            # Output attributes
    T_keys      = set(T_pair_dict.keys());                            # Gold attributes
    TP_keys     = P_keys & T_keys;                                    # Attributes present in output and gold
    P           = sum([len(P_pair_dict[P_key]) for P_key in P_keys]); # Number of attribute-value pairs in output
    T           = sum([len(T_pair_dict[T_key]) for T_key in T_keys]); # Number of attribute-value pairs in gold object
    TP          = 0;                                                  # Number of attribute-value pairs in output and gold
    #for key in P_pair_dict:
    #    print(key,P_pair_dict[key]);
    #print('----------------------------------');
    #for key in T_pair_dict:
    #    print(key,T_pair_dict[key]);
    #print('===================================');
    matches     = [];
    mismatches  = [];
    mapping     = [];
    costs       = [];
    for TP_key in TP_keys:
        prec, rec, TP_, P_, T_, matches_, mismatches_, mapping_, costs_ = compare_refstrings(P_pair_dict[TP_key],T_pair_dict[TP_key],threshold);
        TP                                                             += TP_;
        matches                                                        += [(TP_key,str(match_0),str(match_1),) for match_0,      match_1      in matches_    ];
        mismatches                                                     += [(TP_key,str(match_0),str(match_1),) for match_0,      match_1      in mismatches_ ];
        mapping                                                        += [(TP_key,assignment_0,assignment_1,) for assignment_0, assignment_1 in mapping_    ];
        costs                                                          += [(TP_key,cost_,)                     for cost_                      in costs_      ];
    #print(matches); print('=======================================================================================');
    return TP/P, TP/T, TP, P, T, matches, mismatches, mapping, costs;

def compare_refobjects(P_dicts,T_dicts,threshold):
    P_refstrings                                                       = [P_dict['reference'] for P_dict in P_dicts];
    T_refstrings                                                       = [T_dict['reference'] for T_dict in T_dicts];
    _, _, _, _, _, matches_str, mismatches_str, mapping_str, costs_str = compare_refstrings(P_refstrings,T_refstrings,threshold) if len(P_refstrings)>0 and len(T_refstrings)>0 else [0,0,0,0,0,[],[],[],[]];
    matches_obj, mismatches_obj, mapping_obj, costs_obj = [],[],[],[];
    if matches_str:
        for i,j in mapping_str:
            #P_dict = {key:P_dicts[i][key] if key!='authors' else [{'author_string':[part for part in NAMESEP.split(author['author_string']) if part]} for author in P_dicts[i]['authors'] if 'author_string' in author and author['author_string']] for key in P_dicts[i] if P_dicts[i][key] not in [None,'None',' ',''] };
            #T_dict = {key:T_dicts[j][key] if key!='authors' else [{'author_string':[part for part in NAMESEP.split(author['author_string']) if part]} for author in T_dicts[j]['authors'] if 'author_string' in author and author['author_string']] for key in T_dicts[j] if T_dicts[j][key] not in [None,'None',' ',''] };
            P_dict, T_dict                                                         = P_dicts[i], T_dicts[j];
            #print('===================================');
            #for key in P_dict:
            #    print(key,P_dict[key]);
            #print('----------------------------------');
            #for key in T_dict:
            #    print(key,T_dict[key]);
            #print('===================================');
            _, _, _, _, _, matches_obj_, mismatches_obj_, mapping_obj_, costs_obj_ = compare_refobject(P_dict,T_dict,threshold);
            matches_obj                                                           += [matches_obj_];
            mismatches_obj                                                        += [mismatches_obj_];
            mapping_obj                                                           += [mapping_obj_];
            costs_obj                                                             += [costs_obj_];
    return matches_obj, mismatches_obj, mapping_obj, costs_obj;

def evaluate(threshold):
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=_index,scroll=str(_max_extract_time*_scroll_size)+'m',size=_scroll_size,query=_scr_query);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            _id                                              = doc['_id'];
            # The system produced reference objects taken from the index
            auto_refobjects                                  = doc['_source'][_refobj]           if _refobj           in doc['_source'] and isinstance(doc['_source'][_refobj],list)           else [];
            # The gold annotated reference objects taken from the index
            gold_refobjects                                  = doc['_source']['gold_refobjects'] if 'gold_refobjects' in doc['_source'] and isinstance(doc['_source']['gold_refobjects'],list) else [];
            # The system produced references with more author_string features and no other author features
            auto_refobjects                                  = [{key:D[key] if key!='authors' else [{'author_string':[part for part in NAMESEP.split(author['author_string']) if part]} for author in D['authors'] if 'author_string' in author and author['author_string'] and isinstance(author['author_string'],str)] for key in D if D[key] not in [None,'None',' ',''] } for D in auto_refobjects];
            auto_refobjects                                  = [{key:D[key] if key!='editors' else [{'editor_string':[part for part in NAMESEP.split(author['editor_string']) if part]} for author in D['editors'] if 'editor_string' in author and author['editor_string'] and isinstance(author['editor_string'],str)] for key in D if D[key] not in [None,'None',' ',''] } for D in auto_refobjects];
            # The gold annotated references with more author_string features and no other author features
            gold_refobjects                                  = [{key:D[key] if key!='authors' else [{'author_string':[part for part in NAMESEP.split(author['author_string']) if part]} for author in D['authors'] if 'author_string' in author and author['author_string'] and isinstance(author['author_string'],str)] for key in D if D[key] not in [None,'None',' ',''] } for D in gold_refobjects];
            gold_refobjects                                  = [{key:D[key] if key!='editors' else [{'editor_string':[part for part in NAMESEP.split(author['editor_string']) if part]} for author in D['editors'] if 'editor_string' in author and author['editor_string'] and isinstance(author['editor_string'],str)] for key in D if D[key] not in [None,'None',' ',''] } for D in gold_refobjects];
            # Getting the matches from system to gold to be counted as TP, the rest is irrelevant
            matches_obj,mismatches_obj,mapping_obj,costs_obj = compare_refobjects(auto_refobjects,gold_refobjects,threshold);
            # Creating the system reference attribute values pairs that have already been created and used above
            flat_auto_refobjs                                = [list(pairfy(flatten(auto_refobject))) for auto_refobject in auto_refobjects];
            # Creating the gold reference attribute values pairs that have already been created and used above
            flat_gold_refobjs                                = [list(pairfy(flatten(gold_refobject))) for gold_refobject in gold_refobjects];
            #for flat_auto_refobj in flat_auto_refobjs:
            #    print(flat_auto_refobj,'\n>>>>>>>>>>>>>>>>>>>>>>>>')
            #for match_obj in matches_obj:
            #    print(match_obj,'\n<<<<<<<<<<<<<<<<<<<<<<<<<<<')
            # Summing up TP as number of matches, P as number of system reference attribute value pairs and T as number of gold attribute value pairs
            TP_obj, P_obj, T_obj                             = [sum([len(matches) for matches in refobjects_]) for refobjects_ in [matches_obj,flat_auto_refobjs,flat_gold_refobjs]];
            # Summing up the number of matches per attribute
            TP_keywise                                       = Counter([match[0] for matches in matches_obj for match in matches]);
            #print(TP_keywise)
            # Summing up the number of values per attribute in the system reference attribute value pairs
            P_keywise                                        = Counter([match[0] for matches in flat_auto_refobjs for match in matches]);#sum([Counter({key:len(flat_auto_refobj[key]) for key in flat_auto_refobj if flat_auto_refobj[key]}) for flat_auto_refobj in flat_auto_refobjs],Counter());
            # Summing up the number of values per attribute in the gold reference attribute value pairs
            T_keywise                                        = Counter([match[0] for matches in flat_gold_refobjs for match in matches]);#sum([Counter({key:len(flat_gold_refobj[key]) for key in flat_gold_refobj if flat_gold_refobj[key]}) for flat_gold_refobj in flat_gold_refobjs],Counter());
            print(', '.join([key for key in set(TP_keywise.keys())|set(P_keywise.keys())|set(T_keywise.keys())]));
            body                                             = copy(_body);
            body['_id']                                      = _id;
            body['_source']['doc']['has_results_'+_refobj]   = True if matches_obj else False; # What if auto has some references and gold does not, then not matches_obj
            body['_source']['doc']['results_'+_refobj]       = { 'precision':  TP_obj/P_obj,
                                                                 'recall':     TP_obj/T_obj,
                                                                 'TP':         TP_obj,
                                                                 'P':          P_obj,
                                                                 'T':          T_obj,
                                                                 'keywise':    {key:{'TP':TP_keywise[key] if key in TP_keywise else 0,'P':P_keywise[key] if key in P_keywise else 0,'T':T_keywise[key] if key in T_keywise else 0,'precision':TP_keywise[key]/P_keywise[key] if key in TP_keywise and key in P_keywise else 0,'recall':TP_keywise[key]/T_keywise[key] if key in TP_keywise and key in T_keywise else 0} for key in set(TP_keywise.keys())|set(P_keywise.keys())|set(T_keywise.keys())},
                                                                 'matches':    [match_obj for match_obj in matches_obj    if len(match_obj)>0],
                                                                 'mismatches': [match_obj for match_obj in mismatches_obj if len(match_obj)>0]
                                                               } if matches_obj else None;
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(_max_extract_time*_scroll_size)+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;

_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

_client.indices.put_settings(index=_index, body={"index.mapping.total_fields.limit": 10000});

for refobj in _refobjs:
    _refobj = refobj;
    _body = { '_op_type': 'update',
              '_index': _index,
              '_id': None,
              '_source': {'doc': { 'has_results_'+_refobj: True,
                                   'results_'    +_refobj: None
            }}}
    print('------------------------------------------------------------\n',refobj,'\n------------------------------------------------------------');
    i = 0;
    for success, info in bulk(_client,evaluate(_threshold),chunk_size=_chunk_size):
        i += 1; #print('######',i,'#######################################################################');
        if not success:
            print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
        if i % _chunk_size == 0:
            _client.indices.refresh(index=_index);
    _client.indices.refresh(index=_index);
    #show_results(refobj,_client);
