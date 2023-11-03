import org.gbif.images.ImageMetadata;


import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datavec.image.loader.NativeImageLoader;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.zoo.ZooModel;
import org.deeplearning4j.zoo.model.VGG16;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.nd4j.linalg.api.ndarray.INDArray;

public class ImageEmbeddings {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("ImageEmbeddingsJava")
                                .set("es.nodes", "localhost")
                                .set("es.port", "9200");

    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    SparkSession spark = SparkSession.builder().appName("HBaseSparkSQLExample").getOrCreate();

    Configuration hBaseConfiguration = HBaseConfiguration.create();
    hBaseConfiguration.set("hbase.zookeeper.quorum", "c3zk1.gbif-dev.org:2181,c3zk2.gbif-dev.org:2181,c3zk3.gbif-dev.org:2181");
    HBaseContext hBaseContext = new HBaseContext(jsc.sc(), hBaseConfiguration, null);
    String hbaseTable = "thumbor";

    // Load your image data into a JavaRDD
    Dataset<Row> imageRows = spark.read()
      .option("hbase.table", hbaseTable)
      .option("hbase.columns.mapping", ":key,image:raw")
      .format("org.apache.hadoop.hbase.spark")
      .load();

    // Load a pre-trained model (e.g., Deeplearning4j MultiLayerNetwork)
    MultiLayerNetwork model = loadPretrainedModel();

    // Define a function to extract embeddings
    JavaRDD<String> embeddings = imageRows.javaRDD()
                                    .map(imageRow -> extractEmbedding(imageRow.getAs("raw"), model))
                                    .map(indArray -> indArray.toStringFull());

    JavaEsSpark.saveJsonToEs(embeddings, "your-elasticsearch-index/your-elasticsearch-type");
  }

  @SneakyThrows
  private static MultiLayerNetwork loadPretrainedModel() {
    ZooModel<?> zooModel = VGG16.builder().build();

    // Load the pre-trained model
    return  (MultiLayerNetwork) zooModel.initPretrained();
  }

  @SneakyThrows
  private static INDArray extractEmbedding(byte[] rawImage, MultiLayerNetwork model) {
    // Load and preprocess the image
    ImageMetadata imageMetadata = ImageMetadata.of(rawImage);

    NativeImageLoader loader = new NativeImageLoader(imageMetadata.getHeight(),
                                                     imageMetadata.getWidth(),
                                                     imageMetadata.getChannels());

    INDArray image = loader.asMatrix(rawImage);

    // Perform inference and extract embeddings
    return model.output(image);
  }
}
