from elasticsearch import Elasticsearch
import json
from py2neo import Graph, Node, Relationship

# Создание клиента для связи с Elasticsearch
client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])


graph_db = Graph("bolt://localhost:7687", auth=("neo4j", "1"))

graph_db.delete_all()

write={
"size": 30,
  "query": {
    "match_all": {}
  }
}

teams = client.search(index='teams', body=write)
#print(teams)

for team in teams["hits"]["hits"]:
    try:
        TeamNode = Node("Team", name=int(team['_id']), id = team['_id'], team_info = team['_source']["сведения_о_бригаде"])
        graph_db.create(TeamNode)
    except Exception as e:
        print("ex")
        continue
    searchbody = {
        "size": 30,
        "query": {
            "match_phrase": {
                "id_бригады": {
                    "query": team['_id']
                }
            }
        }
    }

    result = client.search(index='orders', body=searchbody)
#    print(result)

    for order in result["hits"]["hits"]:
        try:
            # Поиск в графе текущего рецепта
            OrderNode = graph_db.nodes.match(
                "Order", Name=order["_source"]["id_заказа"]).first()
            if OrderNode == None:
                OrderNode = Node("Order", name=order["_source"]["id_заказа"], order_info = (order['_source']["id_заказа"]), date = order['_source']["дата_заказа"], customer_info = order['_source']["сведения_о_заказчике"], cost = order['_source']["стоимость_заказа"])
                graph_db.create(OrderNode)
            NodesRelationship = Relationship(OrderNode, "Performed", TeamNode,lead_date=(order['_source']["срок_выполнения_заказа"]),actual_date=(order['_source']["фактическая_дата_выполнения"]))
            graph_db.create(NodesRelationship) 

        except Exception as e:
            print(e)
            continue
