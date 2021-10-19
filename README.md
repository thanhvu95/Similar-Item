### Requirements
java openjdk 1.8.0
scala 2.12.6
spark 3.1.2

### Run driver
Start master:
    start-master.sh
    
Start worker:
    start-worker.sh spark:127.0.1.1:7077
    
Submit spark job:
    spark-submit --master spark:127.0.1.1:7077 --class com.similaritem.SimilarItem /path/to/jar/file /path/to/input/json/file /path/to/output/file sku-name

Path to jar file:
    /SimilarItem/target/scala-2.12/similaritem_2.12-0.1.jar

### sample_result

similar_top_10.csv

Top 10 similar items with sku-123
