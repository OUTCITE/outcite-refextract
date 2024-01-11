#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import re
from elasticsearch import Elasticsearch as ES
from collections import Counter
import tabulate
import time
import numpy as np
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = sys.argv[1]; #'geocite' #'ssoar'
_chunk_size       =  50;
_max_extract_time = 0.1; #minutes
_max_scroll_tries =   2;
_scroll_size      =  25;
_requestimeout    =  60;

_refobjs = set([ 'anystyle_references_from_cermine_fulltext',
                 'anystyle_references_from_cermine_refstrings',
                 'anystyle_references_from_grobid_fulltext',
                 'anystyle_references_from_grobid_refstrings',
                 'anystyle_references_from_pdftotext_fulltext',
                 #'anystyle_references_from_gold_fulltext',
                 #'anystyle_references_from_gold_refstrings',
                 'cermine_references_from_cermine_xml',
                 'cermine_references_from_grobid_refstrings',
                 #'cermine_references_from_gold_refstrings',
                 'grobid_references_from_grobid_xml',
                 'exparser_references_from_cermine_layout',
                 #'merged_references'
 ]);

_methods = { 'grobid_references_from_grobid_xml':           'grobid',
             'cermine_references_from_grobid_refstrings':   'grob.ref--cerm' ,
             'cermine_references_from_gold_refstrings':     'gold.ref--cerm',
             'cermine_references_from_cermine_xml':         'cermine',
             'anystyle_references_from_cermine_fulltext':   'cerm.txt--anyst',
             'anystyle_references_from_pdftotext_fulltext': 'pdftotxt--anyst',
             'anystyle_references_from_cermine_refstrings': 'cerm.ref--anyst',
             'anystyle_references_from_grobid_fulltext':    'grob.txt--anyst',
             'anystyle_references_from_grobid_refstrings':  'grob.ref--anyst',
             'anystyle_references_from_gold_fulltext':      'gold.txt--anyst',
             'anystyle_references_from_gold_refstrings':    'gold.ref--anyst',
             'exparser_references_from_cermine_layout':     'cerm.layo--exp',
             'merged_references':                           'all refobj--merg'};

_targets = ['sowiport','crossref','dnb','openalex','arxiv','ssoar','gesis_bib','research_data'];
_fields  = ['id','url']

INTERESTING = re.compile(r'|'.join([target+'_'+field for target in _targets for field in _fields]));
ID          = re.compile(r'|'.join([target+'_id'     for target in _targets                     ]));
URL         = re.compile(r'|'.join([target+'_url'    for target in _targets                     ]));

tabulate.PRESERVE_WHITESPACE = True

_beta = 1; # importance of recall over precision, default is 1

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

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

def F_beta(T,P,TP,b):
    return (1+b**2) * TP  /  ( (1+b**2) * TP + b**2 * (T-TP) + (P-TP) );

def show_results(index,refobj):
    client               = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    body                 = {'query':{'term':{'has_results_'+refobj: True}},'_source':['results_'+refobj]};
    page                 = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=body);
    sid                  = page['_scroll_id'];
    returned             = len(page['hits']['hits']);
    page_num             = 0;
    TP_str, P_str, T_str = 0,0,0;
    TP_obj, P_obj, T_obj = 0,0,0;
    Prec_str, Rec_str    = 0,0;
    Prec_obj, Rec_obj    = 0,0;
    keywise              = dict();
    number               = 0;
    results_dict         = dict();
    while returned > 0:
        for doc in page['hits']['hits']:
            #if not 'refobj' in doc['_source']['results_'+refobj]:
            #    continue;
            number   += 1;
            TP_str   += doc['_source']['results_'+refobj]['keywise']['_reference']['TP'];#['refobj']
            P_str    += doc['_source']['results_'+refobj]['keywise']['_reference']['P'];
            T_str    += doc['_source']['results_'+refobj]['keywise']['_reference']['T'];
            Prec_str += doc['_source']['results_'+refobj]['keywise']['_reference']['precision'];
            Rec_str  += doc['_source']['results_'+refobj]['keywise']['_reference']['recall'];
            TP_obj   += doc['_source']['results_'+refobj]['TP'];
            P_obj    += doc['_source']['results_'+refobj]['P'];
            T_obj    += doc['_source']['results_'+refobj]['T'];
            Prec_obj += doc['_source']['results_'+refobj]['precision'];
            Rec_obj  += doc['_source']['results_'+refobj]['recall'];
            keywise   = merge(keywise,doc['_source']['results_'+refobj]['keywise']);
            print(doc['_id'],doc['_source']['results_'+refobj]['keywise']['_reference']['TP'],doc['_source']['results_'+refobj]['keywise']['_reference']['P'],doc['_source']['results_'+refobj]['keywise']['_reference']['T'],refobj,);
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
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
        results_dict[refobj] = { 'str': { 'all': {'prec':int(100*TP_str/P_str),    'rec':int(100*TP_str/T_str),   'f1': int(100*F_beta(T_str,P_str,TP_str,_beta))},#int(100*(1+0.5)*(TP_str/P_str)*(TP_str/T_str)/(TP_str/P_str+TP_str/T_str))},
                                          'avg': {'prec':int(100*Prec_str/number), 'rec':int(100*Rec_str/number), 'f1': int(100*(1+0.5)*(Prec_str/number)*(Rec_str/number)/(Prec_str/number+Rec_str/number))} },
                                 'obj': { 'all': {'prec':int(100*TP_obj/P_obj),    'rec':int(100*TP_obj/T_obj),   'f1': int(100*F_beta(T_obj,P_obj,TP_obj,_beta))},#int(100*(1+0.5)*(TP_obj/P_obj)*(TP_obj/T_obj)/(TP_obj/P_obj+TP_obj/T_obj))},
                                          'avg': {'prec':int(100*Prec_obj/number), 'rec':int(100*Rec_obj/number), 'f1': int(100*(1+0.5)*(Prec_obj/number)*(Rec_obj/number)/(Prec_obj/number+Rec_obj/number))} } };
        print('------------------------------------------------------------');
        print('---' + refobj + '---');
        print('------------------------------------------------------------');
        print('REFSTR:','[ALL]','PREC:',int(100*TP_str/P_str),   'REC:',int(100*TP_str/T_str),  'F1:',int(100*F_beta(T_str,P_str,TP_str,_beta)),'TP:',TP_str,'P:',P_str,'T:',T_str);#int(100*2*(TP_str/P_str)*(TP_str/T_str)/(TP_str/P_str+TP_str/T_str)),'TP:',TP_str,'P:',P_str,'T:',T_str);
        print('       ','[AVG]','PREC:',int(100*Prec_str/number),'REC:',int(100*Rec_str/number),'F1:',int(100*2*(Prec_str/number)*(Rec_str/number)/(Prec_str/number+Rec_str/number)));
        print('REFOBJ:','[ALL]','PREC:',int(100*TP_obj/P_obj),   'REC:',int(100*TP_obj/T_obj),  'F1:',int(100*F_beta(T_obj,P_obj,TP_obj,_beta)),'TP:',TP_obj,'P:',P_obj,'T:',T_obj);#int(100*2*(TP_obj/P_obj)*(TP_obj/T_obj)/(TP_obj/P_obj+TP_obj/T_obj)),'TP:',TP_obj,'P:',P_obj,'T:',T_obj);
        print('       ','[AVG]','PREC:',int(100*Prec_obj/number),'REC:',int(100*Rec_obj/number),'F1:',int(100*2*(Prec_obj/number)*(Rec_obj/number)/(Prec_obj/number+Rec_obj/number)));
        for key in keywise:
            key_prec_all = int(100*keywise[key]['TP']/keywise[key]['P']) if keywise[key]['P']>0 else 0;
            key_rec_all  = int(100*keywise[key]['TP']/keywise[key]['T']) if keywise[key]['T']>0 else 0;
            key_f1_all   = int(100*F_beta(keywise[key]['T'],keywise[key]['P'],keywise[key]['TP'],_beta)) if keywise[key]['P']>0 and keywise[key]['T']>0 else 0;#int(100*2*(keywise[key]['TP']/keywise[key]['P'])*(keywise[key]['TP']/keywise[key]['T'])/(keywise[key]['TP']/keywise[key]['P']+keywise[key]['TP']/keywise[key]['T'])) if keywise[key]['P']>0 and keywise[key]['T']>0 else 0;
            #print('\n       ',key+' [ALL]:','PREC:',key_prec_all, 'REC:', key_rec_all, 'F1:',key_f1_all,'TP:', keywise[key]['TP'],'P:', keywise[key]['P'], 'T:', keywise[key]['T']);
            numerator    = int(100*(1+_beta**2)*(keywise[key]['precision']/number)*(keywise[key]['recall']/number));
            denominator  = _beta**2*keywise[key]['precision']/number+keywise[key]['recall']/number;
            key_prec_avg = int(100*keywise[key]['precision']/number);
            key_rec_avg  = int(100*keywise[key]['recall']/number);
            key_f1_avg   = int(numerator/denominator) if denominator>0 else 0;
            #print('       ',''.join([' ' for letter in key])+' [AVG]:','PREC:',key_prec_avg,'REC:',key_rec_avg,'F1:',key_f1_avg);
            results_dict[refobj][key] = { 'all': { 'prec':key_prec_all, 'rec':key_rec_all, 'f1':key_f1_all },
                                          'avg': { 'prec':key_prec_avg, 'rec':key_rec_avg, 'f1':key_f1_avg } };
        print('------------------------------------------------------------');
    client.clear_scroll(scroll_id=sid);
    return results_dict;

def get_overview(index):
    scr_body = { "query": { "match_all": {} } };
    #----------------------------------------------------------------------------------------------------------------------------------
    client             = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page               = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=scr_body);
    sid                = page['_scroll_id'];
    returned           = len(page['hits']['hits']);
    page_num           = 0;
    counts             = Counter();
    denoms             = dict();
    counts['num_docs'] = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            counts['num_docs'] += 1;
            for key in doc['_source']:
                if key.startswith('has_') and doc['_source'][key]:
                    counts[key] += 1;
                    denoms[key]  = 'num_docs';
                elif key in _refobjs and doc['_source'][key] and len(doc['_source'][key])>0:
                    counts[key+'.any_ref'] += 1;
                    denoms[key+'.any_ref']  = 'num_docs';
                    for reference in doc['_source'][key]:
                        counts[key] += 1;
                        denoms[key]  = key;
                        has_id       = 0;
                        has_url      = 0;
                        for key_ in reference:
                            if INTERESTING.match(key_):
                                counts[key+'.'+key_] += 1;
                                denoms[key+'.'+key_]  = key;
                                if ID.match(key_):
                                    has_id = 1;
                                elif URL.match(key_):
                                    has_url = 1;
                        counts[key+'.any_id']  += has_id;
                        denoms[key+'.any_id']   = key;
                        counts[key+'.any_url'] += has_url;
                        denoms[key+'.any_url']  = key;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
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
    return counts,denoms;

def get_difference(value,measure,results_dict,key1,key2,methods_list):
    best = max([results_dict[refobj][key1][key2][measure] for refobj in methods_list if refobj in results_dict]);
    diff = 0 if best==0 else value/best - 1;
    return diff if diff < 0 else value/100;

def color(integer):
    if integer < 0:
        return '\\textcolor{red}{'+str(-1*integer)+'}';
    return '\\textcolor{blue}{'+str(integer)+'}';

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

counts, denoms = get_overview(_index);
print(tabulate.tabulate([[key,counts[key],round(counts[key]*100/counts[denoms[key]],0)] for key in sorted(list(counts.keys())) if key!='num_docs'],headers=['field','count','%']));

results_dict = dict();
for refobj in _refobjs:
    results_dict = merge(results_dict,show_results(_index,refobj));

for key1 in results_dict['grobid_references_from_grobid_xml']:
    for key2 in results_dict['grobid_references_from_grobid_xml'][key1]:
        print('--------------------------------------------------------------------------------------------------')
        print(' '.join([' ' for i in range(35)]),key1,'--->',key2);
        print('--------------------------------------------------------------------------------------------------');
        print(tabulate.tabulate([[refobj,results_dict[refobj][key1][key2]['prec'],results_dict[refobj][key1][key2]['f1'],results_dict[refobj][key1][key2]['rec']] if key1 in results_dict[refobj] else [refobj,'-','-','-'] for refobj in results_dict],headers=['method','P','F1','R']));

_keys = { '_reference':                  'refstr',
          '_title':                      'title' ,
          '_year':                       'year',
          'authors_author_string':       'author',
          'editors_editor_string':       'editor',
          'publishers_publisher_string': 'publ',
          '_source':                     'source',
          '_volume':                     'vol',
          '_issue':                      'issue',
          '_start':                      'startp',
          '_end':                        'endp'};
_keyslist = list(_keys.keys());

_methods_list = sorted(list(_refobjs));

# compact print
for key2 in ['avg']:
    print('--------------------------------------------------------------------------------------------------')
    print(' '.join([' ' for i in range(35)]),key1,'--->',key2);
    print('--------------------------------------------------------------------------------------------------');
    table = [   [ _methods[refobj] ]
              + [ str(results_dict[refobj][key1][key2]['prec']).rjust(2,' ')+' | '+str(results_dict[refobj][key1][key2]['f1']).rjust(2,' ')+' | '+str(results_dict[refobj][key1][key2]['rec']).rjust(2,' ') if key1 in results_dict[refobj] else [refobj,'-','-','-'] for key1 in _keyslist ]
                for refobj in _methods_list if refobj in results_dict];
    print(tabulate.tabulate(table,headers=['method']+[_keys[key] for key in _keyslist]));

# latex table
for key2 in ['avg']:
    print('--------------------------------------------------------------------------------------------------')
    print(' '.join([' ' for i in range(35)]),key1,'--->',key2);
    print('--------------------------------------------------------------------------------------------------');
    f1s   = {key:[results_dict[refobj][key][key2]['f1'] for refobj in _methods_list if refobj in results_dict and refobj!='merged_references'] for key in _keyslist};
    wins  = {key:np.flatnonzero(f1s[key] == np.max(f1s[key])) for key in f1s};
    table = [   [ _methods[_methods_list[i]] ]
              + [ str(results_dict[_methods_list[i]][key1][key2]['prec'])+' & '+[str(results_dict[_methods_list[i]][key1][key2]['f1']),'\\textcolor{blue}{'+str(results_dict[_methods_list[i]][key1][key2]['f1'])+'}'][i in wins[key1]]+' & '+str(results_dict[_methods_list[i]][key1][key2]['rec']) if key1 in results_dict[_methods_list[i]] else [_methods_list[i],'-','-','-'] for key1 in _keyslist ]
                for i in range(len(_methods_list)) if _methods_list[i] in results_dict];
    #print(' & '.join(['method']+[_keys[key]+' '+measure for key in _keyslist for measure in ['P','F1','R']]),'\\\\');
    print("\\begin{tabular}{l|"+'|'.join(['rrr' for key1 in _keyslist])+"|}");
    print("\\multicolumn{1}{c|}{\\multirow{2}{*}{\\textbf{"+_index.upper()+"}}} & "+' & '.join(["\\multicolumn{3}{c|}{\\textbf{"+_keys[key1]+"}}" for key1 in _keyslist])+" \\\\");
    print("\\multicolumn{1}{c|}{}  & "+' & '.join(["\\multicolumn{1}{c}{P} & \\multicolumn{1}{c}{F1} & \\multicolumn{1}{c|}{R}" for key1 in _keyslist])+" \\\\\n\hline");
    for row in table:
        print(' & '.join(row),'\\\\');
    print("\\hline\n\\end{tabular}");

# compact print divergence from best
for key2 in ['avg']:
    print('--------------------------------------------------------------------------------------------------')
    print(' '.join([' ' for i in range(35)]),key1,'--->',key2);
    print('--------------------------------------------------------------------------------------------------');
    table = [   [ _methods[refobj] ]
              + [   str(int(100*get_difference(results_dict[refobj][key1][key2]['prec'], 'prec', results_dict,key1,key2,_methods_list))).rjust(4,' ') + '|'
                  + str(int(100*get_difference(results_dict[refobj][key1][key2]['f1']  , 'f1'  , results_dict,key1,key2,_methods_list))).rjust(4,' ') + '|'
                  + str(int(100*get_difference(results_dict[refobj][key1][key2]['rec'] , 'rec',  results_dict,key1,key2,_methods_list))).rjust(4,' ')
                if key1 in results_dict[refobj] else [refobj,'-','-','-'] for key1 in _keyslist ]
                for refobj in _methods_list if refobj in results_dict];
    print(tabulate.tabulate(table,headers=['method']+[_keys[key] for key in _keyslist]));

# latex table divergence from best
for key2 in ['avg']:
    print('--------------------------------------------------------------------------------------------------')
    print(' '.join([' ' for i in range(35)]),key1,'--->',key2);
    print('--------------------------------------------------------------------------------------------------');
    table = [   [  _methods[refobj] ]
              + [   str(color(int(100*get_difference(results_dict[refobj][key1][key2]['prec'], 'prec', results_dict,key1,key2,_methods_list)))) +' & '
                  + str(color(int(100*get_difference(results_dict[refobj][key1][key2]['f1']  , 'f1'  , results_dict,key1,key2,_methods_list)))) +' & '
                  + str(color(int(100*get_difference(results_dict[refobj][key1][key2]['rec'] , 'rec',  results_dict,key1,key2,_methods_list))))
                if key1 in results_dict[refobj] else [refobj,'-','-','-'] for key1 in _keyslist ]
                for refobj in _methods_list if refobj in results_dict];
    #print(','.join(['method']+[_keys[key]+' '+measure for key in _keyslist for measure in ['P','F1','R']]),'\\\\');
    print("\\begin{tabular}{l|"+'|'.join(['rrr' for key1 in _keyslist])+"|}");
    print("\\multicolumn{1}{c|}{\\multirow{2}{*}{\\textbf{"+_index.upper()+"}}} & "+' & '.join(["\\multicolumn{3}{c|}{\\textbf{"+_keys[key1]+"}}" for key1 in _keyslist])+" \\\\");
    print("\\multicolumn{1}{c|}{}  & "+' & '.join(["\\multicolumn{1}{c}{P} & \\multicolumn{1}{c}{F1} & \\multicolumn{1}{c|}{R}" for key1 in _keyslist])+" \\\\\n\hline");
    for row in table:
        print(' & '.join(row),'\\\\');
    print("\\hline\n\\end{tabular}");

# weight dictionary
table = { refobj: { _keys[key]:results_dict[refobj][key]['avg']['f1'] for key in _keyslist if key in results_dict[refobj]} for refobj in _methods_list if refobj in results_dict};
print(table)
#-------------------------------------------------------------------------------------------------------------------------------------------------
