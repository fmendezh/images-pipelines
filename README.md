# images-pipelines



sudo -u hdfs spark2-submit --class org.gbif.images.ImageEmbeddingsPipeline --master yarn --executor-memory 8G  --driver-memory 4G  --num-executors 20 --executor-cores 5  --conf spark.dynamicAllocation.enabled=true  --deploy-mode=client --conf spark.default.parallelism=100 images-pipelines-1.0-SNAPSHOT.jar