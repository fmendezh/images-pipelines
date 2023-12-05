package org.gbif.images;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.gbif.images.ImageUtils.downloadInatImage;

@Slf4j
public class ImageCrawler {

    public static void main(String[] args) {

        String filePath = args[0];
        String imageStorePath = args[1];
        String errorImagesPath = args[2];
        int batchSize = Integer.parseInt(args[3]);
        int numThreads = Integer.parseInt(args[4]);

        crawl(filePath, imageStorePath, errorImagesPath, batchSize, numThreads);
    }

    @SneakyThrows
    public static void crawl(String filePath, String imageStorePath, String errorImagesPath, int batchSize, int numThreads) {
        List<String> imageUrls = Files.readAllLines(Paths.get(filePath));
        List<String> failedImages = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        // Process URLs in batches
        for (int i = 0; i < imageUrls.size(); i += batchSize) {
            int to = Math.min(i + batchSize, imageUrls.size());
            List<String> batch = imageUrls.subList(i, to);
            log.info("Crawling images batch from {} to {}", i, to);

            // Submit download tasks for each image URL in the batch
            for (String row : batch) {
                String[] cols = row.split("\t");
                String classKey = cols[cols.length -1];
                String imageUrl = cols[0];
                executorService.submit(() -> {
                    try {
                        Path imageStoreClassPath = Paths.get(imageStorePath, classKey);
                        Files.createDirectories(imageStoreClassPath);
                        downloadInatImage(imageUrl, imageStoreClassPath.toString());
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

            // Create a new executor for the next batch
            executorService = Executors.newFixedThreadPool(numThreads);
        }
        // Shutdown the executor when all tasks are completed
        executorService.shutdown();
        if (!failedImages.isEmpty()) {
            saveRowsToFile(failedImages, errorImagesPath);
        }

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
}
