package org.gbif.images;

import lombok.SneakyThrows;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import static org.gbif.images.ImageUtils.getInatSmallImageFileName;

public class SparkImageCrawler {
    public static void main(String[] args) {
        String warehouseLocation = args[0];
        String selectStatement = args[1];
        String imageStorePath =  args[2];

        SparkSession spark = SparkSession.builder()
                .appName("ImageCrawler")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .getOrCreate();

        // Read data from Hive table
        Dataset<Row> hiveData = spark.sql(selectStatement);

        // Extract image URLs from the DataFrame
        JavaRDD<String> imageUrls = hiveData.javaRDD().map(row -> row.getString(0));

        // Parallelize image download logic using foreachPartition
        imageUrls.foreachPartition(urlIterator -> {
            // Initialize any resources needed for parallel download
            while (urlIterator.hasNext()) {
                String imageUrl = urlIterator.next();
                // Implement your image download logic here
                downloadImage(imageUrl, imageStorePath);
            }
        });
        spark.stop();
    }

    @SneakyThrows
    private static void downloadImage(String originalImageUrl, String imageStorePath) {
        URL imageUrl = new URI(originalImageUrl).toURL();
        try (InputStream imageReader = new BufferedInputStream(imageUrl.openStream());
             OutputStream imageWriter = new BufferedOutputStream(Files.newOutputStream(Paths.get(imageStorePath, getInatSmallImageFileName(originalImageUrl))));)
        {
            byte[] b = new byte[2048];
            int length;

            while ((length = imageReader.read(b)) != -1) {
                imageWriter.write(b, 0, length);
            }
        }
    }
}