import json
from elasticsearch import Elasticsearch
#from elasticsearch-7.17.0/bin/elasticsearch import Elasticsearch
client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
indexName = "orders"
client.indices.delete(index=indexName)
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

#client.indices.delete(index=indexName)
client.indices.create(index=indexName, body=index_settings)

mapping = {
"properties": {
"id_заказа": {
"type": "integer"#,
#3"analyzer": "my_custom_analyzer"
},
"дата_заказа": {
"type": "date",
"format":"YYYY-mm-dd"
#"analyzer": "my_custom_analyzer"
},
"id_заказчика": {
"type": "integer"#,
#"analyzer": "my_custom_analyzer"
},
"сведения_о_заказчике": {
"type": "text",
"analyzer": "my_custom_analyzer"
},
"данные_о_заказе": {
"type": "text",
"analyzer": "my_custom_analyzer"
},
"срок_выполнения_заказа": {
"type": "date",
"format":"YYYY-mm-dd"
#"analyzer": "my_custom_analyzer"
},
"фактическая_дата_выполнения": {
"type": "date",
"format":"YYYY-mm-dd"
#"analyzer": "my_custom_analyzer"
},
"стоимость_заказа": {
"type": "integer"#,
#"analyzer": "my_custom_analyzer"
},
"id_бригады": {
"type": "integer"#,
#"analyzer": "my_custom_analyzer"
},
}
}
client.indices.put_mapping(index=indexName, doc_type="orders_info", include_type_name="true", body=mapping)
with open('orders1.json', 'r') as f:
    dataStore = json.load(f)
for data in dataStore:
    for field in data["body"]:
        if (field != "id_бригады" and field != "стоимость_заказа" and  field != "фактическая_дата_выполнения"  and  field != "срок_выполнения_заказа"  and  field !=  "id_заказчика" and  field !=  "дата_заказа" and  field !=  "id_заказа" ):
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
