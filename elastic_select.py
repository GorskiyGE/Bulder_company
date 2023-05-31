from elasticsearch import Elasticsearch
import pprint
client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
indexName = "orders"
docType = "orders_info"
searchFrom = 0
searchSize = 3
# 1
searchBody = {
"_source": ["information_about_customer"],
"query": {
"simple_query_string": {
"query": 'Смирнов'
}
}
}
result = client.search(index=indexName, body=searchBody, from_=searchFrom, size=searchSize)



pprint.pprint(result)
