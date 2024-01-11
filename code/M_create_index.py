#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import time
import json
from elasticsearch import Elasticsearch as ES
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
#_mapping = sys.argv[1];
_name       = sys.argv[1];
_num_shards = int(sys.argv[2]) if len(sys.argv)>2 else 1;

_body = { 'settings' : { 'index' : { 'number_of_shards': _num_shards } } };
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

index_name = _name+'-'+time.ctime(time.time()).replace(' ','-').replace(':','').lower();

client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
#client = ES(['http://search.gesis.org/es-config'],scheme='http',port=80,timeout=60);

#IN      = open(_mapping);
#mapping = json.load(IN);
#IN.close();

#indices = set(client.indices.get(_name)) | set(client.indices.get(_name+'-*')) | set(client.indices.get_alias(_name+"*"));
indices = set(client.indices.get(index=_name+'-*')) | set(client.indices.get_alias(index=_name+"*"));
for index in indices:
    #if index != _name:
    print('...deleting old index', index);
    client.indices.delete(index=index, ignore=[400, 404]);

response = client.indices.create(index=index_name, body=_body);#, body=mapping );
print('created new index', index_name);
if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print("INDEX MAPPING SUCCESS.");
elif 'error' in response:
    print("ERROR:", response['error']['root_cause']);
    print("TYPE:", response['error']['type']);

client.indices.put_alias(index=index_name, name=_name);
print('added alias "',_name,'" to index',index_name);
#-------------------------------------------------------------------------------------------------------------------------------------------------
