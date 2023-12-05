package org.gbif.images;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.RowFactory;
import org.deeplearning4j.nn.graph.ComputationGraph;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ImageEmbeddingsPipeline {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("ImageEmbeddingsJava")
                                .set("es.nodes", "localhost")
                                .set("es.port", "9200");

    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    SparkSession spark = SparkSession.builder().appName("HBaseSparkSQLExample").enableHiveSupport().getOrCreate();

    Configuration hBaseConfiguration = HBaseConfiguration.create();
    hBaseConfiguration.set("hbase.zookeeper.quorum", "c4zk1.gbif-uat.org,c4zk2.gbif-uat.org,c4zk3.gbif-uat.org");
    String hdfsModelPath = "hdfs://ha-nn/user/fmendez/vgg16/coefficients.bin";
    String hbaseTable = "thumbor";

    // Descargar el modelo desde HDFS
    byte[] modelBytes = downloadModelFromHDFS(hdfsModelPath);

    // Distribuir el modelo a los nodos ejecutores
    List<byte[]> modelBytesList = new ArrayList<>();
    for (int i = 0; i < spark.sparkContext().defaultParallelism(); i++) {
      modelBytesList.add(modelBytes);
    }

    JavaRDD<byte[]> modelBytesRDD = jsc.parallelize(modelBytesList);

    Dataset<Row> imageCache = spark.sql("SELECT * FROM fede.occurrence_image_cache LIMIT 10");

    JavaRDD<Row> embedddings = imageCache.javaRDD()
            .mapPartitions(new HBaseMapPartitionsFunction(HBaseConfiguration.create(hBaseConfiguration), hbaseTable, "image".getBytes(), "raw".getBytes()));
  }

  static class HBaseMapPartitionsFunction implements FlatMapFunction<Iterator<Row>, Row> {
    private final org.apache.hadoop.conf.Configuration hbaseConfig;
    private final ImageEmbedding imageEmbedding;

    private final String tableName;

    private final byte[] columnFamily;

    private final byte[] qualifier;

    public HBaseMapPartitionsFunction(org.apache.hadoop.conf.Configuration hbaseConfig, String tableName, byte[] columnFamily,  byte[] qualifier) {
      this.hbaseConfig = hbaseConfig;
      this.imageEmbedding = new ImageEmbedding();
      this.tableName = tableName;
      this.columnFamily = columnFamily;
      this.qualifier = qualifier;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> partition) throws Exception {
      // Create an HBase connection
      try (Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
        // Create an empty list to store HBase results
        List<Result> results = new ArrayList<>();

        // Iterate over the partition and fetch rows from HBase
        List<Row> predictions = new ArrayList<>();
        while (partition.hasNext()) {
          Row imageCacheRow = partition.next();
          Result result = getHBaseRow(connection, tableName, imageCacheRow.getString(3));
          byte[] image = result.getValue(columnFamily, qualifier);
          INDArray embedding = imageEmbedding.extractEmbedding(image);

          //oc.datasetkey, oc.gbifid, om.identifier, i.key AS cache_key
          predictions.add(RowFactory.create(imageCacheRow.getString(3), imageCacheRow.getString(0),
                                            imageCacheRow.getString(1), imageCacheRow.getString(2),
                                            serializeINDArray(embedding)));
        }

        return predictions.iterator();

      }
    }

    @SneakyThrows
    private Result getHBaseRow(Connection connection, String tableName, String rowKey) {
      try (Table table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))) {
        Get get = new Get(rowKey.getBytes());
        return table.get(get);
      }
    }
  }

  public static byte[] serializeINDArray(INDArray array) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutput out = new ObjectOutputStream(bos)) {
      // Serialize the INDArray to a byte array
      out.writeObject(array);
      return bos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Error serializing INDArray", e);
    }
  }

  public static INDArray deserializeINDArray(byte[] serializedArray) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedArray);
         ObjectInput in = new ObjectInputStream(bis)) {
      // Deserialize the byte array back to an INDArray
      return (INDArray) in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Error deserializing INDArray", e);
    }
  }

  @SneakyThrows
  private static byte[] downloadModelFromHDFS(String hdfsPath) {
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    FileSystem fs = FileSystem.get(URI.create(hdfsPath), hadoopConf);
    Path path = new Path(hdfsPath);

    // Leer el modelo desde HDFS
    byte[] modelBytes;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos);
         FSDataInputStream inputStream = fs.open(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        bos.write(buffer, 0, bytesRead);
      }
      modelBytes = bos.toByteArray();
    }

    return modelBytes;
  }

  @SneakyThrows
  private static byte[] serializeModel(ComputationGraph model) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(model);
      return bos.toByteArray();
    }
  }

  @SneakyThrows
  private static ComputationGraph deserializeModel(byte[] modelBytes) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(modelBytes);
         ObjectInputStream ois = new ObjectInputStream(bis)) {
      return (ComputationGraph) ois.readObject();
    }
  }
}
