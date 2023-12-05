package org.gbif.images;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ImageRow {

    private String identifier;
    private String datasetKey;
    private String gbifId;
    private String classKey;
    private String photoId;
    private String fileExtension;
    private double[] embedding;

    public static ImageRow fromRow(String row, String separator) {
        return fromRow(row, null, separator);
    }

    public static ImageRow fromRow(String row, double[] embedding, String separator) {
        String[] cols = row.split(separator);
        String identifier = cols[0];
        return ImageRow.builder()
                .identifier(identifier)
                .datasetKey(cols[1])
                .gbifId(cols[2])
                .classKey(cols[3])
                .photoId(getPhotoId(identifier))
                .fileExtension(getFileExtension(identifier))
                .embedding(embedding)
                .build();
    }

    public static ImageRow from(ImageRow row, double[] embedding) {
        return ImageRow.builder()
                .identifier(row.identifier)
                .datasetKey(row.datasetKey)
                .gbifId(row.gbifId)
                .classKey(row.classKey)
                .photoId(row.photoId)
                .fileExtension(row.fileExtension)
                .embedding(embedding)
                .build();
    }

    public static ImageRow fromTsvRow(String row) {
        return fromRow(row, "\t");
    }

    public static ImageRow fromTsvRow(String row, double[] embeddings) {
        return fromRow(row, embeddings, "\t");
    }

    private static String getPhotoId(String url) {
        String[] parts = url.split("/");
        return parts[parts.length - 2];
    }

    private static String getFileExtension(String url) {
        if (url.contains(".")) {
            String[] parts = url.split("\\.");
            return parts[parts.length -1];
        }
        return url;
    }
}
