import json
from elasticsearch import Elasticsearch
#from elasticsearch-7.17.0/bin/elasticsearch import Elasticsearch
client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
indexName = "teams"
#client.indices.delete(index=indexName)
#client.indices.create(index=indexName)

# Создание индекса с указанными настройками
index_settings = {
  "settings": {
    "analysis": {
      "filter": {
        "my_stopwords": {
          "type": "stop",
          "stopwords": "_russian_"
        },
        "my_snowball": {
          "type": "snowball",
          "language": "Russian"
        }
      },
      "analyzer": {
        "my_custom_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "my_stopwords",
            "my_snowball"
          ]
        }
      }
    }
  }
}
client.indices.delete(index=indexName)
client.indices.create(index=indexName, body=index_settings)
mapping = {
"properties": {
"сведения_о_бригаде": {
"type": "text",
"analyzer": "my_custom_analyzer",
"fielddata": True
},
"член_бригады": {
"type": "text",
"fielddata": True
#"analyzer": "my_custom_analyzer"
},
"отзыв_о_работе": {
"type": "text",
"analyzer": "my_custom_analyzer",
"fielddata": True
},
}
}
client.indices.put_mapping(index=indexName, doc_type="teams_info", include_type_name="true", body=mapping)
with open('brigades1.json', 'r') as f:
    dataStore = json.load(f)
for data in dataStore:
    for field in data["body"]:
        if (field != "член_бригады"):
            analyzed_doc = client.indices.analyze(index=indexName, body={"text": data["body"][field], "analyzer": "my_custom_analyzer"})
            analyzed_text = [token["token"] for token in analyzed_doc["tokens"]]
            data["body"][field] = analyzed_text
    try:
        client.index(
        index=data["index"],
        doc_type=data["doc_type"],
        id=data["id"],
        body=data["body"])
    except Exception as e:
        print(e)
