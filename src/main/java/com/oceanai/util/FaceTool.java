package com.oceanai.util;

import com.google.gson.Gson;
import com.oceanai.model.CompareInfo;
import com.oceanai.model.ExtractResult;
import com.oceanai.model.FaceFeature;
import com.oceanai.model.SearchFeature;
import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public  class FaceTool {

    private static FaceTool instance = null;
    public static String jpyHome = "/usr/local/jpy/jpyconfig.properties";
    private PyObject face_tool;
    private static float threshold = 0.75f;
    private Gson gson = new Gson();

    public static FaceTool getInstance(){
        if (instance == null)
//            return new FaceTool("/home/hadoop/wrp/FaceDetect/config/search.json");
            return new FaceTool("/home/hadoop/storm-projects/lib/config/search.json");
        else
            return instance;
    }

    public FaceTool(String configPath) {
        System.setProperty("jpy.config", jpyHome);
        PyLib.startPython("/home/test/face-atlas/python");
        PyModule module = PyModule.importModule("face");
        face_tool = module.call("FACE", configPath, 0, 0);
    }

    public FaceTool() {

    }

    public List<SearchFeature> extract(ImageInfo image) {
        List<SearchFeature> faceFeatures = new ArrayList<SearchFeature>();
        float[][] rawFeatures = face_tool.call("extract", image.pixels, image.height, image.width)
                .getObjectArrayValue(float[].class);
        for (int i = 0; i < rawFeatures.length; ++i) {
            faceFeatures.add(new SearchFeature(rawFeatures[i]));
        }
        return faceFeatures;
    }

    public List<SearchFeature> extract(String base64) {
        String raw = HttpClientUtil.post(base64);
        ExtractResult er = gson.fromJson(raw, ExtractResult.class);
        List<SearchFeature> searchFeatures = new ArrayList<>();
        for (int i = 0;i<er.getFace_nums();i++) {
            FaceFeature faceFeature = er.getResult()[i];
            int x1 = faceFeature.getLeft();
            int y1 = faceFeature.getTop();
            int x2 = x1 + faceFeature.getWidth();
            int y2 = y1 + faceFeature.getHeight();
            SearchFeature searchFeature = new SearchFeature(x1, y1, x2, y2, 0, faceFeature.getFeature());
            searchFeatures.add(searchFeature);
        }
        return searchFeatures;
    }

    public static CompareInfo getSamePersons(List<float[]> features, List<String> ids, float[] queryFeature) {
        CompareInfo compareInfo = new CompareInfo();
        if (features == null || ids == null || queryFeature == null
                || features.size() <= 0 || ids.size() <= 0)
            return null;
        Map<String, Float> scoreMap = new HashMap<String, Float>();
        String id = null;
        for (int i = 0; i < features.size(); i++) {
            float res = cosineSimilarity(features.get(i), queryFeature) - threshold;
            String key = ids.get(i);
            Float value = scoreMap.get(key);
            if (value == null) value = new Float(0.0f);

            if (res > 0) {
                scoreMap.put(key, value + 0.7f * res);
            } else {
                scoreMap.put(key, value + 0.3f * res);
            }
        }
        Map.Entry<String, Float> maxEntry = null;
        for (Map.Entry<String, Float> entry : scoreMap.entrySet()) {
            if (maxEntry == null || entry.getValue() > maxEntry.getValue()) {
                maxEntry = entry;
            }
        }
        if (maxEntry != null && maxEntry.getValue() > 0) {
            id = maxEntry.getKey();
            compareInfo.setSimilarity(maxEntry.getValue());
        }
        compareInfo.setmId(id);
        return compareInfo;
    }

    public static float cosineSimilarity(float[] vectorA, float[] vectorB) {
        float dotProduct = 0.0f;
        float normA = 0.0f;
        float normB = 0.0f;
        for (int i = 0; i < vectorA.length; i++) {
            dotProduct += vectorA[i] * vectorB[i];
            normA += Math.pow(vectorA[i], 2);
            normB += Math.pow(vectorB[i], 2);
        }
        return (float) (0.5 + 0.5 * (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB))));
    }

    public static byte[] imageToBytes(BufferedImage image, String encoding) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, encoding, baos);
        return baos.toByteArray();
    }

    public static BufferedImage bytesToImage(byte[] buf) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        return ImageIO.read(bais);
    }

    public static byte[] decodeToPixels(BufferedImage bufferedImage) {
        if (bufferedImage == null)
            return null;
        return ((DataBufferByte) bufferedImage.getRaster().getDataBuffer()).getData();
    }

    public static BufferedImage getImageFromArray(byte[] pixels, int width, int height) {
        if (pixels == null || width <= 0 || height <= 0)
            return null;
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
        byte[] array = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
        System.arraycopy(pixels, 0, array, 0, array.length);
        return image;
    }

    public static boolean isTheSamePerson(float[] feature1, float[] feature2) {
        if (feature1 == null || feature2 == null
                || feature1.length == 0 || feature2.length == 0)
            return false;

        return cosineSimilarity(feature1, feature2) > threshold;
    }

    public static float getThreshold() {
        return threshold;
    }

    public static void setThreshold(float threshold) {
        FaceTool.threshold = threshold;
    }

    public static class ImageInfo {
        public byte[] pixels;
        public int width;
        public int height;

        public ImageInfo(byte[] pixels, int width, int height) {
            this.pixels = pixels;
            this.width = width;
            this.height = height;
        }

        public ImageInfo (BufferedImage image) throws Exception {
            this.pixels = ((DataBufferByte)image.getRaster().getDataBuffer()).getData();
            this.width = image.getWidth();
            this.height = image.getHeight();
        }
    }
}
