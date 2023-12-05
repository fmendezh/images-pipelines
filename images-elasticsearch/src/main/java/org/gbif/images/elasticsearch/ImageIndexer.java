package org.gbif.images.elasticsearch;

import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.images.ImageRow;

import java.util.List;

@Slf4j
public class ImageIndexer {

    private final EsClient.EsClientConfiguration configuration;

    private final String indexName;


    public ImageIndexer(EsClient.EsClientConfiguration configuration, String indexName) {
        this.configuration = configuration;
        this.indexName = indexName;
    }

    public void createIndex() {
        try (EsClient esClient = new EsClient(EsClient.provideElasticsearchTransport(configuration))) {
            esClient.createIndex(indexName, IndexingConstants.DEFAULT_INDEXING_SETTINGS, "mapping.json");
        }
    }

    public void finishIndexing() {
        try (EsClient esClient = new EsClient(EsClient.provideElasticsearchTransport(configuration))) {
            esClient.updateSettings(indexName, IndexingConstants.DEFAULT_SEARCH_SETTINGS);
            esClient.flushIndex(indexName);
        }
    }

    @SneakyThrows
    public void index(List<ImageRow> images) {
        try (EsClient esClient = new EsClient(EsClient.provideElasticsearchTransport(configuration))) {
            BulkRequest.Builder bulkRequestBuilder = new BulkRequest.Builder();
            images.forEach(imageRow -> bulkRequestBuilder.operations(op -> op.index(idx -> idx.index(indexName)
                    .id(imageRow.getIdentifier())
                    .document(imageRow))));
            BulkResponse response = esClient.bulk(bulkRequestBuilder.build());
            if (response.errors()) {
                log.error("Error indexing in elastic {}", response);
            } else {
                log.info("BulkRequest succeeded indexed {} documents", response.items().size());
            }
        } catch (Exception ex) {
            log.error("Error indexing in elastic", ex);
            throw new RuntimeException(ex);
        }
    }
}
