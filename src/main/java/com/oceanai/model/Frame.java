package com.oceanai.model;

import com.oceanai.util.FaceTool;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Frame {

    public final static String NO_IMAGE = "none";
    public final static String JPG_IMAGE = "jpg";

    private String streamId;
    private long timeStamp;
    private String imageType = JPG_IMAGE;
    private byte[] imageBytes;
    private BufferedImage image;
    private Rectangle boundingBox;
    private String text = "flag";
    private long sequenceNr;

    private List<SearchFeature> faceSearchFeatures = new ArrayList<>();

    private byte[] boxedImage;
    private BufferedImage boxedBufferedImage;

    /**
     * 此方法设置streamId和sequenceNr
     */
    public Frame(String streamId, long sequenceNr, String imageType, byte[] image, byte[] image_copy, long timeStamp,
                 Rectangle boundingBox) {
        this.streamId = streamId;
        this.sequenceNr = sequenceNr;
        this.imageType = imageType;
        this.boxedImage = image_copy;
        this.imageBytes = image;
        this.timeStamp = timeStamp;
        this.boundingBox = boundingBox;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public long getSequenceNr() {
        return sequenceNr;
    }

    public void setFaceSearchFeatures(List<SearchFeature> faceSearchFeatures) {
        this.faceSearchFeatures = faceSearchFeatures;
    }

    public BufferedImage getBoxedBufferedImage() throws IOException {
        if (boxedImage == null) {
            //imageType = NO_IMAGE;
            return null;
        }
        if (this.boxedBufferedImage == null) {
            this.boxedBufferedImage = FaceTool.getImageFromArray(boxedImage, (int) boundingBox.getWidth(), (int)
                    boundingBox.getHeight());
        }
        return this.boxedBufferedImage;
    }

    public void setBoxedBufferedImage(BufferedImage bufferedImage) {
        this.boxedBufferedImage = bufferedImage;
        if (bufferedImage != null) {
            this.boxedImage = FaceTool.decodeToPixels(bufferedImage);
        } else {
            this.boxedImage = null;
        }
    }

    public long getTimestamp() {
        return this.timeStamp;
    }

    public byte[] getImageBytes() {
        return imageBytes;
    }
}

