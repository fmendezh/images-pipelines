package org.gbif.images;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.gbif.images.elasticsearch.EsClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ImagesWsConfiguration {

    @Bean
    public ImageEmbedding imageEmbedding() {
        return new ImageEmbedding();
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(ImageAppConfiguration configuration) {
        return new ElasticsearchClient(EsClient.provideElasticsearchTransport(configuration.getElasticsearch()));
    }

    @ConfigurationProperties
    @Bean
    public ImageAppConfiguration configuration() {
        return new ImageAppConfiguration();
    }
}
