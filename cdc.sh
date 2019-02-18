echo "STARTS THE SQOOP PROCESS FOR CDC LOGIC"
sh sqoop_cdc.sh
echo "ENDS THE SQOOP PROCESS FOR CDC LOGIC"
echo "####################################"
echo "STARTS THE SPARK PROCESS FOR CDC LOGIC"
spark-submit spark_cdc.py
echo "ENDS THE SPARK PROCESS FOR CDC LOGIC"
echo "####################################"
