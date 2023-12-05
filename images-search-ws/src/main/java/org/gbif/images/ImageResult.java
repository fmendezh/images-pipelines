package org.gbif.images;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ImageResult {

    private String identifier;
    private String datasetKey;
    private String gbifId;
    private String photoId;
    private String fileExtension;


}
