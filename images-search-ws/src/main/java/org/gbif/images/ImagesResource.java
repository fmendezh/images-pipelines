package org.gbif.images;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.KnnQuery;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.elasticsearch.core.search.SourceFilter;
import lombok.SneakyThrows;
import org.gbif.api.model.common.paging.PagingResponse;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.imageio.ImageIO;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import static org.gbif.images.ImageUtils.downloadImage;
import static org.gbif.images.ImageUtils.toByteArray;
import static org.gbif.images.ImageUtils.getFileExtension;


@RestController
@RequestMapping(value = "images", produces = MediaType.APPLICATION_JSON_VALUE)
public class ImagesResource {

    private static final SourceConfig SOURCE_CONFIG = SourceConfig.of( scb -> scb.filter(SourceFilter.of(b -> b.includes(Arrays.asList("identifier", "datasetKey", "gbifId", "photoId", "fileExtension")))));

    private static final int NUM_CANDIDATES = 100;

    private final ImageEmbedding imageEmbedding;

    private final ElasticsearchClient elasticsearchClient;

    private final ImageAppConfiguration configuration;

    @Autowired
    @SneakyThrows
    public ImagesResource(ImageEmbedding imageEmbedding, ElasticsearchClient elasticsearchClient, ImageAppConfiguration configuration) {
        this.imageEmbedding = imageEmbedding;
        this.elasticsearchClient = elasticsearchClient;
        this.configuration = configuration;
        Files.createDirectories(Paths.get(configuration.getDownloadPath()));
    }

    @RequestMapping(value = "/search", method = { RequestMethod.GET,RequestMethod.POST })
    @SneakyThrows
    public  PagingResponse<ImageResult> search(@RequestParam(name="imageFile", required = false) MultipartFile imageFile,
                                               @RequestParam(name="imageUrl", required = false) String imageUrl,
                                               @RequestParam(name="limit", defaultValue="10") Integer limit,
                                               @RequestParam(name="offset", defaultValue="0") Long offset) {

        byte[] imageBytes = getPreferredSource(imageFile, imageUrl);

        try (INDArray embedding = imageEmbedding.extractEmbedding(imageBytes)) {
            KnnQuery knnQuery = knnQuery(limit, embedding);

            SearchResponse<ImageResult> searchResponse = search(knnQuery, offset, limit);

            return toPagingResponse(searchResponse, offset, limit);
        }
    }

    @SneakyThrows
    private byte[] getPreferredSource(MultipartFile imageFile, String imageUrl) {
        if (imageFile != null) {
            return imageFile.getBytes();
        } else if (imageUrl != null) {
            return downloadAndRead(imageUrl);
        }
        throw new IllegalArgumentException("At least one of the parameters, imageFile or imageUrl, must be specified.");
    }

    @SneakyThrows
    private byte[] downloadAndRead(String imageUrl) {
        Path imageDownloadPath = downloadImage(imageUrl, configuration.getDownloadPath());
        try {
            return toByteArray(ImageIO.read(imageDownloadPath.toFile()), getFileExtension(imageUrl));
        } finally {
            imageDownloadPath.toFile().delete();
        }
    }


    @SneakyThrows
    private  SearchResponse<ImageResult> search(KnnQuery query, Long offset, Integer limit) {
        return elasticsearchClient.search(new SearchRequest.Builder()
                .knn(query)
                .from(offset.intValue())
                .index(configuration.getIndexName())
                .size(limit)
                .source(SOURCE_CONFIG)
                .build(), ImageResult.class);
    }
    private static KnnQuery knnQuery(int k, INDArray embedding) {
        return new KnnQuery.Builder()
                .field("embedding")
                .queryVector(toFloatVector(embedding))
                .k(k)
                .numCandidates(NUM_CANDIDATES)
                .build();
    }

    private static PagingResponse<ImageResult> toPagingResponse(SearchResponse<ImageResult> searchResponse, Long offset, Integer limit) {
        if (searchResponse.hits().total() != null) {
            return new PagingResponse<>(offset, limit, searchResponse.hits().total().value(), hitsToList(searchResponse));
        }
        return new PagingResponse<>(offset,limit, 0L);
    }

    @PostMapping("/{identifier}")
    @SneakyThrows
    public  ImageResult get(@PathVariable(name="identifier") String identifier) {
        GetResponse<ImageResult> image = elasticsearchClient.get(new GetRequest.Builder()
                                                                    .id(identifier)
                                                                    .index(configuration.getIndexName())
                                                                    .build(), ImageResult.class);
        return image.source();
    }

    public static List<Float> toFloatVector(INDArray embedding) {
        double[] a = ImageEmbedder.normalizeVector(embedding.toDoubleVector());

        List<Float> floatList = new ArrayList<>();
        for (double d : a) {
            floatList.add((float)d);
        }
        return floatList;
    }

    private static List<ImageResult> hitsToList(SearchResponse<ImageResult> searchResponse) {
        return searchResponse.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
    }
}
