package org.gbif.images;

import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Iterator;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.metadata.IIOMetadataNode;
import javax.imageio.stream.ImageInputStream;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

@Data
@Builder
public class ImageMetadata {

  private int width;

  private int height;

  private int channels;

  @SneakyThrows
  public static ImageMetadata of(byte[] rawImage){
    ImageInputStream imageInputStream = ImageIO.createImageInputStream(rawImage);
    Iterator<ImageReader> readers = ImageIO.getImageReaders(imageInputStream);
    if (readers.hasNext()) {
      ImageReader reader = readers.next();
      return ImageMetadata
              .builder()
              .width(reader.getWidth(0))
              .height(reader.getHeight(0))
              .channels(numberOfChannels(reader))
              .build();

    }
    throw new RuntimeException("Image metadata not found!");
  }

  @SneakyThrows
  private static int numberOfChannels(ImageReader reader) {
    IIOMetadata metadata = reader.getImageMetadata(0);
    if (metadata.isStandardMetadataFormatSupported()) {
      IIOMetadataNode root = (IIOMetadataNode) metadata.getAsTree(metadata.getNativeMetadataFormatName());
      IIOMetadataNode colorSpaceType = findNode(root, "ColorSpaceType");
      if (colorSpaceType != null) {
        return Integer.parseInt(colorSpaceType.getAttribute("NumChannels"));
      }
    }
    return reader.getRawImageType(0).getNumComponents();
  }

  private static IIOMetadataNode findNode(IIOMetadataNode root, String nodeName) {
    for (int i = 0; i < root.getLength(); i++) {
      if (root.item(i) instanceof IIOMetadataNode) {
        IIOMetadataNode node = (IIOMetadataNode) root.item(i);
        if (node.getNodeName().equalsIgnoreCase(nodeName)) {
          return node;
        }
      }
    }
    return null;
  }
}
