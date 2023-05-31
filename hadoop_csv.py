# -*- coding: utf-8 -*- 
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession

def write_to_hdfs(self, table, name):
    df = self.createDataFrame(table)
    df.write.mode('overwrite').options(header='true').csv(f'hdfs://localhost:9000/1/{name}.csv')
    #df.write.csv(f"hdfs://localhost:9000/1/{name}.csv")


SparkSession.write_to_hdfs = write_to_hdfs
client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])



write={
"size": 30,
  "query": {
    "match_all": {}
  }
}

teams = []
orders = []
customers = []

teams_el = client.search(index='teams', body=write)
#print(teams)

for team in teams_el["hits"]["hits"]:
    teams.append({'team_id': team['_id'], 'team_info': ' '.join(team['_source']["сведения_о_бригаде"])})

orders_el = client.search(index='orders', body=write)
for order in orders_el["hits"]["hits"]:

    orders.append({"id_order": order["_source"]["id_заказа"], "date":  order['_source']["дата_заказа"], "customer_id":  order['_source']["id_заказчика"], "cost": order['_source']["стоимость_заказа"]})
            
    customers.append({"customer_id": order['_source']["id_заказчика"], "customer_info": ' '.join(order['_source']["сведения_о_заказчике"])})

ss = SparkSession.builder.appName('csv_write').getOrCreate()
ss.write_to_hdfs(teams, 'teams')
ss.write_to_hdfs(orders, 'orders')
ss.write_to_hdfs(customers, 'customers')


for name in ['teams', 'orders', 'customers']:
    df_load = ss.read.options(header='true').csv(f'hdfs://localhost:9000/1/{name}.csv')
    #df_load = ss.read.csv(f'hdfs://localhost:9000/1/{name}.csv')
    df_load.show(30, False)
input("Ctrl C")
