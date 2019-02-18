import sys
import os
from pyspark.sql import *
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
#rootLogger = Logger.getRootLogger()
#rootLogger.setLevel(Level.ERROR)
print("Setting up Spark Conf and sc starts")
conf = (SparkConf()
.setAppName("spark_cdc")
.set("spark.dynamicAllocation.enabled","true")
.set("spark.shuffle.service.enabled", "true")
.set("spark.rdd.compress", "true"))
sc = SparkContext(conf =conf)
sqlContext = HiveContext(sc)
print("Setting up Spark Conf and sc ends ")
input= "/user/jobyjohny/spark/customers/*parquet*"
parquetFile = sqlContext.read.parquet(input)
parquetFile.registerTempTable("customers_extract");
select_sql = "SELECT * FROM customers_extract order by customer_id desc limit 10"
sqlContext.sql(select_sql).show()
delete_sql = "DROP TABLE IF EXISTS retail_db_hive.customers_new"
sqlContext.sql(delete_sql)

create_sql = """
CREATE TABLE retail_db_hive.customers_new
(
  customer_id       int
 ,customer_fname    varchar(45)
 ,customer_lname    varchar(45)
 ,customer_email    varchar(45)
 ,customer_password varchar(45)
 ,customer_street   varchar(45)
 ,customer_city     varchar(45)
 ,customer_state    varchar(45)
 ,customer_zipcode  varchar(45)
)
STORED AS PARQUET
"""
sqlContext.sql(create_sql)
delta1_sql = """
INSERT OVERWRITE TABLE retail_db_hive.customers_new
select
  a.customer_id
 ,a.customer_fname   
 ,a.customer_lname    
 ,a.customer_email    
 ,a.customer_password
 ,a.customer_street  
 ,a.customer_city     
 ,a.customer_state    
 ,a.customer_zipcode
  from retail_db_hive.customers a left join customers_extract b 
  on (a.customer_id=b.customer_id) where b.customer_id is null
"""
sqlContext.sql(delta1_sql)

print("delta1 process ends ")
delta2_sql = """
INSERT INTO TABLE retail_db_hive.customers_new
select
  a.customer_id
 ,a.customer_fname   
 ,a.customer_lname    
 ,a.customer_email    
 ,a.customer_password
 ,a.customer_street  
 ,a.customer_city     
 ,a.customer_state    
 ,a.customer_zipcode
  from customers_extract a
"""
sqlContext.sql(delta2_sql)
print("delta2 process ends ")
select_sql = "SELECT * FROM retail_db_hive.customers_new order by customer_id desc limit 10"
sqlContext.sql(select_sql).show()
print("CDC process ends ")

