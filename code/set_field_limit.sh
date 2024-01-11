index=$1;

curl -X PUT "localhost:9200/${index}/_settings" -H 'Content-Type: application/json' -d'
{
  "index.mapping.total_fields.limit": 5000
}
'
