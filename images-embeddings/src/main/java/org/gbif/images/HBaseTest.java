package org.gbif.images;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.nd4j.linalg.api.ndarray.INDArray;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class HBaseTest {

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();

    // Specify the HBase ZooKeeper quorum (you may need to change this)
    conf.set("hbase.zookeeper.quorum", "c3zk1.gbif-dev.org:2181,c3zk2.gbif-dev.org:2181,c3zk3.gbif-dev.org:2181");
    ImageEmbedding imageEmbedding = new ImageEmbedding();

    try (Connection connection = ConnectionFactory.createConnection(conf);
         Table sourceTable = connection.getTable(TableName.valueOf("thumbor"));
         Table targetTable = connection.getTable(TableName.valueOf("image_embeddings"))) {

      Scan scan = new Scan();
      scan.setCaching(20);
      //result.getValue(Bytes.toBytes("image"), Bytes.toBytes("raw"))
      //Bytes.toString(result.getRow()).substring(33)
      ResultScanner scanner = sourceTable.getScanner(scan);


      List<Put> puts = new ArrayList<>();
      int batchSize = 50;
      for (Result result : scanner) {
        // Process the row data here
        System.out.println("Row key: " + Bytes.toString(result.getRow()));
        // You can access column values like this:
        // byte[] value = result.getValue(Bytes.toBytes("family"), Bytes.toBytes("column"));
        INDArray embedding = imageEmbedding.extractEmbedding(result.getValue(Bytes.toBytes("image"), Bytes.toBytes("raw")));
        Put put = new Put(result.getRow());
        put.addColumn("image".getBytes(), "embedding".getBytes(), serializeINDArray(embedding));
        puts.add(put);

        if (puts.size() >= batchSize){
          targetTable.put(puts);
          puts.clear();
        }
      }
      if (!puts.isEmpty()) {
        targetTable.put(puts);
      }

    } catch (Exception e) {
      e.printStackTrace();
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
}
