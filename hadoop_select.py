from pyspark.sql import SparkSession

ss = SparkSession.builder.appName('read_csv').getOrCreate()
ss.read.options(header='true').csv('hdfs://localhost:9000/1/orders.csv').createOrReplaceTempView('orders')
ss.read.options(header='true').csv('hdfs://localhost:9000/1/customers.csv').createOrReplaceTempView('customers')

res_df = ss.sql('''
select c.customer_id, c.customer_info, SUM(o.cost)
from orders o
join customers c on c.customer_id = o.customer_id
GROUP BY 1,2
ORDER BY 3 desc
''')

res_df.show()
input("Ctrl C")
