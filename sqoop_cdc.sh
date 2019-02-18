HDFS_FILE_PATH=/user/jobyjohny/spark/customers
hadoop fs -rm -r -skipTrash $HDFS_FILE_PATH
sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_export" --username retail_user --password itversity \
--table customers --target-dir $HDFS_FILE_PATH --as-parquetfile --compress --compression-codec snappy -m 1