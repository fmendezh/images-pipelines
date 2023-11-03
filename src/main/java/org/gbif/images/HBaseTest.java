package org.gbif.images;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();

    // Specify the HBase ZooKeeper quorum (you may need to change this)
    conf.set("hbase.zookeeper.quorum", "c3zk1.gbif-dev.org:2181,c3zk2.gbif-dev.org:2181,c3zk3.gbif-dev.org:2181");

    try (Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(TableName.valueOf("thumbor"))) {

      Scan scan = new Scan();
      // Set the number of rows you want to retrieve (N)
      int N = 10;
      scan.setCaching(N);
      //result.getValue(Bytes.toBytes("image"), Bytes.toBytes("raw"))
      //Bytes.toString(result.getRow()).substring(33)
      ResultScanner scanner = table.getScanner(scan);
      int rowCount = 0;

      for (Result result : scanner) {
        // Process the row data here
        System.out.println("Row key: " + Bytes.toString(result.getRow()));
        // You can access column values like this:
        // byte[] value = result.getValue(Bytes.toBytes("family"), Bytes.toBytes("column"));

        rowCount++;

        if (rowCount >= N) {
          break;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
