package org.gbif.images;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.images.elasticsearch.EsClient;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ImageAppConfiguration {

    private String indexName;
    private String downloadPath;
    private EsClient.EsClientConfiguration elasticsearch;

}
