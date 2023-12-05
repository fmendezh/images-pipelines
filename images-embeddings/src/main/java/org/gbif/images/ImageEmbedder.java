package org.gbif.images;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.images.elasticsearch.EsClient;
import org.gbif.images.elasticsearch.ImageIndexer;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
@Data
@Builder
public class ImageEmbedder {

    public static void main(String[] args) {

        String sourceFilePath = args[0];
        String imageStorePath = args[1];
        String errorImagesPath = args[2];
        String outputFilePath = args[3];
        int batchSize = Integer.parseInt(args[4]);
        int numThreads = Integer.parseInt(args[5]);

        ImageEmbedder.builder()
                .sourceFilePath(sourceFilePath)
                .imageStorePath(imageStorePath)
                .errorImagesPath(errorImagesPath)
                .outputFilePath(outputFilePath)
                .batchSize(batchSize)
                .numThreads(numThreads)
                .imageEmbedding(new ImageEmbedding())
                .build()
                .crawl();
    }


    private final String sourceFilePath;

    private final ImageEmbedding imageEmbedding;
    private final String imageStorePath;
    private final String errorImagesPath;
    private final String outputFilePath;
    private final int batchSize;
    private final int numThreads;


    private EsClient.EsClientConfiguration esConfiguration() {
        EsClient.EsClientConfiguration configuration = new EsClient.EsClientConfiguration();
        configuration.setHosts("http://fede-vh.gbif-dev.org:9200/");
        configuration.setConnectionTimeOut(60_000);
        configuration.setSocketTimeOut(60_000);
        configuration.setConnectionRequestTimeOut(60_000);
        return configuration;
    }

    @SneakyThrows
    public void crawl() {
        List<String> imageUrls = Files.readAllLines(Paths.get(sourceFilePath));
        List<String> failedImages = new ArrayList<>();
        ImageIndexer imageIndexer = new ImageIndexer(esConfiguration(),"images");
        imageIndexer.createIndex();
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        // Process URLs in batches
        for (int i = 0; i < imageUrls.size(); i += batchSize) {
            int to = Math.min(i + batchSize, imageUrls.size());
            List<String> batch = imageUrls.subList(i, to);
            List<ImageRow> embeddings = new ArrayList<>();
            log.info("Calculating  embeddings for image batch from {} to {}", i, to);

            // Submit download tasks for each image URL in the batch
            for (String row : batch) {
                String[] cols = row.split("\t");
                String classKey = cols[cols.length -1];
                String imageUrl = cols[0];
                executorService.submit(() -> {
                    try {
                        Path imageStoreClassPath = Paths.get(imageStorePath, classKey);
                        Files.createDirectories(imageStoreClassPath);
                        embeddings.add(calculateEmbedding(row));
                    } catch (Exception ex) {
                        failedImages.add(row);
                        log.error("Error crawling image {}", imageUrl, ex);
                    }
                });
            }

            // Wait for the current batch to complete before processing the next batch
            executorService.shutdown();
            while (!executorService.isTerminated()) {
                // Waiting for all tasks to complete
            }
            imageIndexer.index(embeddings);

            // Create a new executor for the next batch
            executorService = Executors.newFixedThreadPool(numThreads);
        }
        // Shutdown the executor when all tasks are completed
        executorService.shutdown();
        if (!failedImages.isEmpty()) {
            saveRowsToFile(failedImages, errorImagesPath);
        }
        imageIndexer.finishIndexing();
        //saveRowsToFile(embeddings, outputFilePath);

    }

    @SneakyThrows
    private ImageRow calculateEmbedding(String row) {
        ImageRow imageRow = ImageRow.fromTsvRow(row);
        try(INDArray embedding = imageEmbedding.extractEmbedding(Files.readAllBytes(getImageFile(imageRow)))) {
            imageRow.setEmbedding(normalizeVector(embedding.toDoubleVector()));
            return imageRow;
        }
    }

    private Path getImageFile(ImageRow imageRow) {
        return Paths.get(imageStorePath, imageRow.getClassKey(), ImageUtils.getInatImageFileName(imageRow.getIdentifier()));
    }

    @SneakyThrows
    private static void saveRowsToFile(List<String> rows, String filePath) {
        Path path = Paths.get(filePath);
        Files.createDirectories(path.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            for (String row : rows) {
                writer.write(row);
                writer.newLine(); // Add a new line after each string
            }
        }
    }

    public static double[] normalizeVector(double[] vector) {
        double[] normalizedVector = new double[vector.length];
        double magnitude = 0.0;
        for (int i = 0; i < vector.length; i++) {
            magnitude += Math.pow(vector[i], 2);
        }
        magnitude = Math.sqrt(magnitude);
        for (int i = 0; i < vector.length; i++) {
            normalizedVector[i] = vector[i] / magnitude;
        }
        return normalizedVector;
    }
}
