package org.gbif.images;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class ImageUtils {

    public static String getInatImageFileName(String url) {
        String[] parts = url.split("/");
        String photoId = parts[parts.length - 2];
        String fileName = parts[parts.length - 1];
        return photoId + "." + getFileExtension(fileName);
    }

    public static String getFileName(String url) {
        String[] parts = url.split("/");
        return parts[parts.length - 1];
    }

    public static String getPhotoId(String url) {
        String[] parts = url.split("/");
        return  parts[parts.length - 2];
    }

    public static String getFileExtension(String fileName) {
        if (fileName.contains(".")) {
            String[] parts = fileName.split("\\.");
            return parts[parts.length -1];
        }
        return fileName;
    }

    public static String getInatSmallImageFileName(String url) {
        return url.replace("original.", "small.");
    }


    @SneakyThrows
    public static void downloadInatImage(String originalImageUrl, String imageStorePath) {
        URL imageUrl = new URI(getInatSmallImageFileName(originalImageUrl)).toURL();
        try (InputStream imageReader = new BufferedInputStream(imageUrl.openStream());
             OutputStream imageWriter = new BufferedOutputStream(Files.newOutputStream(Paths.get(imageStorePath, getInatImageFileName(originalImageUrl)))))
        {
            byte[] b = new byte[2048];
            int length;

            while ((length = imageReader.read(b)) != -1) {
                imageWriter.write(b, 0, length);
            }
        }
    }

    @SneakyThrows
    public static Path downloadImage(String originalImageUrl, String imageStorePath) {
        URL imageUrl = new URI(getInatSmallImageFileName(originalImageUrl)).toURL();
        Path imagePath = Paths.get(imageStorePath, System.currentTimeMillis() + "_" + getFileName(originalImageUrl));
        try (InputStream imageReader = new BufferedInputStream(imageUrl.openStream());
             OutputStream imageWriter = new BufferedOutputStream(Files.newOutputStream(imagePath)))
        {
            byte[] b = new byte[2048];
            int length;

            while ((length = imageReader.read(b)) != -1) {
                imageWriter.write(b, 0, length);
            }

            return imagePath;
        }
    }

    @SneakyThrows
    public static byte[] toByteArray(BufferedImage bi, String format) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ImageIO.write(bi, format, baos);
            return baos.toByteArray();
        }
    }

    public static BufferedImage resizeImage(BufferedImage originalImage, int newWidth, int newHeight) {
        // Create a new BufferedImage with the desired size
        BufferedImage resizedImage = new BufferedImage(newWidth, newHeight, originalImage.getType());

        // Get the Graphics2D object from the resized image
        Graphics2D g2d = resizedImage.createGraphics();

        // Draw the original image onto the resized image
        g2d.drawImage(originalImage, 0, 0, newWidth, newHeight, null);

        // Dispose the Graphics2D object to free resources
        g2d.dispose();

        return resizedImage;
    }
}
