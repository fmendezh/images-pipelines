package org.gbif.images;

import lombok.SneakyThrows;
import org.datavec.image.loader.NativeImageLoader;
import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.zoo.model.VGG16;
import org.nd4j.linalg.api.ndarray.INDArray;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;

import static org.gbif.images.ImageUtils.resizeImage;

public class ImageEmbedding {

    public static final int IMG_MODEL_LENGTH = 224;

    private final ComputationGraph model;

    public ImageEmbedding(ComputationGraph model) {
        this.model = model;
    }

    public ImageEmbedding() {
        this.model = loadPretrainedModel();
    }

    @SneakyThrows
    private static ComputationGraph loadPretrainedModel() {
        VGG16 zooModel = VGG16.builder().build();

        // Load the pre-trained model
        return  (ComputationGraph)zooModel.initPretrained();
    }


    @SneakyThrows
    public INDArray extractEmbedding(byte[] rawImage) {

        //Size VGG16 input
        BufferedImage image = resizeImage(ImageIO.read(new ByteArrayInputStream(rawImage)), IMG_MODEL_LENGTH, IMG_MODEL_LENGTH);

        // Load and preprocess the image
        ImageMetadata imageMetadata = ImageMetadata.of(image);

        NativeImageLoader loader = new NativeImageLoader(imageMetadata.getHeight(),
                imageMetadata.getWidth(),
                imageMetadata.getChannels());

        INDArray imageIndArray = loader.asMatrix(image);

        // Perform inference and extract embeddings
        return model.output(imageIndArray)[0];
    }

}
