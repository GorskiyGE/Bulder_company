# Bulder_company
Курсовая работа, Elasticsearch, Kibana, Ne04j, Spark, Hadoop
## Задание
В Elasticsearch создать индекс с анализатором и маппингом, проиндексировать json-документы, разработать запросы с вложенной агрегацией, представить результаты в среде Kibana. В Neo4j по данным из Elasticsearch заполнить графовую базу данных, разработать и реализовать запрос к этой БД. В Spark по данным из Elasticsearch сформировать csv-файлы (с внутренней схемой) таблиц и сохранить их в файловой системе HDFS, написать запрос и реализовать его в Spark, проанализировать процесс выполнения запроса с использованием монитора.
# Elastcicsearch
## Mapping 
Приведен пример mapping для бригад
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
     },
     "отзыв_о_работе": {
     "type": "text",
     "analyzer": "my_custom_analyzer",
     "fielddata": True
     },
     }
     }
## Filtr
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
} }
} }
}
## Запрос 1
Разбить заказы по дате заказа с периодом 1 год, для каждой группы определить суммарную стоимость заказов по каждой бригаде
 GET orders/_search
     {
       "size": 0,
       "aggs": {
         "orders_by_year": {
           "date_histogram": {
             "field": "дата_заказа",
             "calendar_interval": "1y"
           },
           "aggs": {
             "cost_by_team": {
               "terms": {
                 "field": "id_бригады"
}, "aggs": {
                 "total_cost": {
                   "sum": {
                     "field": "стоимость_заказа"
                   }
} }
} }
} }
}
## Запрос 2
Предложить признаки отрицательного отзыва; определить бригады хотя бы с одним отрицательным отзывом
 GET teams/_search
{
"size": 0,
           "aggs": {
             "negative_reviews": {
               "filter": {
                 "terms": {
                   "отзыв_о_работе":    ["плох",
"неправд"]
}
}, "aggs": {
                 "teams_with_negative":{
                   "terms": {
                     "field": "_id",
                       "min_doc_count": 1,
                       "size": 60
} }
} }
} }
# Neo4j
## Запрос
MATCH (o:`Order`)-[p:Performed]->(t:Team)
WHERE p.actual_date < p.lead_date
RETURN o.name, p.lead_date, p.actual_date, t.id, t.team_info;
# Spark
## Запрос
 from pyspark.sql import SparkSession
     //Создание Spark сессии и чтение csv-файлов
     ss = SparkSession.builder.appName('read_csv').getOrCreate()
     ss.read.options(header='true').csv('hdfs://localhost:9000/1/
     orders.csv').createOrReplaceTempView('orders')
     ss.read.options(header='true').csv('hdfs://localhost:9000/1/
     customers.csv').createOrReplaceTempView('customers')
     //запрос SQL
     res_df = ss.sql('''
     select c.customer_id, c.customer_info, SUM(o.cost)
     from orders o
     join customers c on c.customer_id = o.customer_id
     GROUP BY 1,2
     ORDER BY 3 desc
     ''')
     // вывод таблицы в терминал
     res_df.show()
     input("Ctrl C")
# Планы запросов 
== Parsed Logical Plan ==
GlobalLimit 21
+- LocalLimit 21
+- Project [cast(customer_id#28 as string) AS customer_id#40, cast(customer_info#29 as string) AS customer_info#41, cast(sum(CAST(cost AS DOUBLE))#33 as string) AS sum(CAST(cost AS DOUBLE))#42]
+- Sort [sum(CAST(cost AS DOUBLE))#33 DESC NULLS LAST], true
+- Aggregate [customer_id#28, customer_info#29], [customer_id#28, customer_info#29, sum(cast(cost#10 as double)) AS sum(CAST(cost AS DOUBLE))#33]
csv
+- Join Inner, (customer_id#28 = customer_id#11)
:- SubqueryAlias `o`
: +- SubqueryAlias `orders`
: +- Relation[cost#10,customer_id#11,date#12,id_order#13]
+- SubqueryAlias `c`
+- SubqueryAlias `customers`
+- Relation[customer_id#28,customer_info#29] csv
== Analyzed Logical Plan ==
customer_id: string, customer_info: string, sum(CAST(cost AS DOUBLE)): string GlobalLimit 21
+- LocalLimit 21
+- Project [cast(customer_id#28 as string) AS customer_id#40, cast(customer_info#29 as string) AS customer_info#41, cast(sum(CAST(cost AS DOUBLE))#33 as string) AS sum(CAST(cost AS DOUBLE))#42]
+- Sort [sum(CAST(cost AS DOUBLE))#33 DESC NULLS LAST], true
+- Aggregate [customer_id#28, customer_info#29], [customer_id#28, customer_info#29, sum(cast(cost#10 as double)) AS sum(CAST(cost AS DOUBLE))#33]
csv
+- Join Inner, (customer_id#28 = customer_id#11)
:- SubqueryAlias `o`
: +- SubqueryAlias `orders`
: +- Relation[cost#10,customer_id#11,date#12,id_order#13]
+- SubqueryAlias `c`
+- SubqueryAlias `customers`
+- Relation[customer_id#28,customer_info#29] csv


== Optimized Logical Plan == GlobalLimit 21
+- LocalLimit 21
+- Project [customer_id#28, customer_info#29, cast(sum(CAST(cost AS DOUBLE))#33 as string) AS sum(CAST(cost AS DOUBLE))#42]
+- Sort [sum(CAST(cost AS DOUBLE))#33 DESC NULLS LAST], true
+- Aggregate [customer_id#28, customer_info#29], [customer_id#28, customer_info#29, sum(cast(cost#10 as double)) AS sum(CAST(cost AS DOUBLE))#33]
csv
+- Project [cost#10, customer_id#28, customer_info#29] +- Join Inner, (customer_id#28 = customer_id#11)
:- Project [cost#10, customer_id#11]
: +- Filter isnotnull(customer_id#11)
: +- Relation[cost#10,customer_id#11,date#12,id_order#13]
+- Filter isnotnull(customer_id#28)
+- Relation[customer_id#28,customer_info#29] csv


== Physical Plan ==
TakeOrderedAndProject(limit=21, orderBy=[sum(CAST(cost AS DOUBLE))#33 DESC NULLS LAST], output=[customer_id#28,customer_info#29,sum(CAST(cost AS DOUBLE))#42])
+- *(3) HashAggregate(keys=[customer_id#28, customer_info#29], functions=[sum(cast(cost#10 as double))], output=[customer_id#28, customer_info#29, sum(CAST(cost AS DOUBLE))#33])
+- Exchange hashpartitioning(customer_id#28, customer_info#29, 200)
+- *(2) HashAggregate(keys=[customer_id#28, customer_info#29], functions=[partial_sum(cast(cost#10 as double))], output=[customer_id#28,
customer_info#29, sum#47])
+- *(2) Project [cost#10, customer_id#28, customer_info#29]
+- *(2) BroadcastHashJoin [customer_id#11], [customer_id#28], Inner, BuildLeft
:- BroadcastExchange HashedRelationBroadcastMode(List(input[1, string, true]))
: +- *(1) Project [cost#10, customer_id#11]
: +- *(1) Filter isnotnull(customer_id#11)
: +- *(1) FileScan csv [cost#10,customer_id#11] Batched:
false,  Format:     CSV:  Location: [],
InMemoryFileIndex[hdfs://localhost:9000/1/orders.csv], PartitionFilters: [], PushedFilters: [IsNotNull(customer_id)], ReadSchema: struct<cost:string,customer_id:string>
+- *(2) Project [customer_id#28, customer_info#29] +- *(2) Filter isnotnull(customer_id#28)
+- *(2) FileScan csv [customer_id#28,customer_info#29] Batched: false, Format: CSV, Location: InMemoryFileIndex[hdfs://localhost:9000/1/customers.csv], PartitionFilters: [], PushedFilters: [IsNotNull(customer_id)], ReadSchema:
struct<customer_id:string,customer_info:string>
