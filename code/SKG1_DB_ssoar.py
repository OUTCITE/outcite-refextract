# -*- coding: utf-8 -*-
#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
from urllib.request import urlopen
import json
import sys
import sqlite3
import re
import ssl
from collections import Counter
import numpy as np
from scipy.sparse import csr_matrix as csr
from SKG_common import *
from elasticsearch import Elasticsearch as ES
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_MAP = sys.argv[1];
_OUT = sys.argv[2];
_GWS = len(sys.argv) > 3 and sys.argv[3].lower() == 'gws';

_prefix            = 'ssoar';
_root_field_origin = 'handle' if not _GWS else 'id';
_root_field_target = 'id';

_batch  = 1000;
_maxlen = 999999999999999999999;

_query = None; # Here you can add a query that retrieves a specific subset of documents based on given IDs
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SPECIFIC-FUNCTIONS------------------------------------------------------------------------------------------------------------------------------
def get_items_ssoar(batch): #TODO: Error 503
    print('Start scrolling...');
    context   = ssl._create_unverified_context();
    processed = 0;
    items     = [];
    start     = 0;
    length    = batch;
    while length > 0:
        processed += length; print('GESIS-SSOAR: Number of items scrolled over:', processed);
        connection = urlopen("https://ssoar.svko-dda-test.gesis.intra/solr/search/select?q=search.resourcetype%3A+2&start="+str(start)+"&rows="+str(batch)+"&wt=json",context=context);
        response   = json.load(connection);
        length     = len(response['response']['docs']);
        start     += batch;
        items     += response['response']['docs'];
        if processed > 1000: break;
        print(100.*start/response['response']['numFound']);
    return items;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------
mapping, index2target, target2index, origin_map = get_mapping(_MAP);

items = get_items_ssoar(_batch) if not _GWS else list(get_items_gws(_batch,_maxlen,"GESIS-SSOAR","publication",_query));
rows  = convert(items,_prefix,origin_map,_root_field_origin,_root_field_target,target2index);

insert_rows(rows,index2target,_OUT);
#-------------------------------------------------------------------------------------------------------------------------------------------------

