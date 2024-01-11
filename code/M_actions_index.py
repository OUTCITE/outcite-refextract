import json
import requests

_index = 'outcite_ssoar'
_address = 'http://svko-skg.gesis.intra:9200'


def update_index(method, url, body):
    payload = json.dumps(body)
    headers = {
      'Content-Type': 'application/json'
    }
    response = requests.request(method, url, headers=headers, data=payload)
    print(response.text)

# ________________________________________________________________________
# _____________________ For deleting a field______________________________
delete_method = 'POST'
_delete_function = '_update_by_query'
delete_url = _address + '/' + _index + '/' + _delete_function
_field_to_delete = 'has_grobid_references_from_grobid_xml'
_field_to_delete_indicator = 'has_grobid_references_from_grobid_xml'
delete_body = {
      "script": "ctx._source.remove('" + _field_to_delete + "')",  # field to be deleted
      "query": {  # search docs for the field
        "bool": {
          "must": {
            "term": {
              _field_to_delete_indicator: True
            }
          }
        }
      }
    }
update_index(delete_method, delete_url, delete_body)

# ________________________________________________________________________
# ________________ For increasing no. of fields __________________________
# increase_field_limit_method = 'PUT'
# _limit_function = '_settings'
# increase_field_limit_url = _address + '/' + _index + '/' + _limit_function
# increase_field_limit_body = {
#     "index.mapping.total_fields.limit": 2000
# }
# update_index(increase_field_limit_method, increase_field_limit_url, increase_field_limit_body)
