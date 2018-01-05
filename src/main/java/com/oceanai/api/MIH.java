package com.oceanai.api;

import com.oceanai.model.CompareInfo;
import com.oceanai.util.FileLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by WangRupeng on 17-11-15.
 */
public class MIH {

    private static float threshold = 0.75f;
    private static FileLogger fileLogger = new FileLogger("/home/hadoop/wrp/RealtimeStreamPublish/java/logs/MIH.log");

    public static void setThreshold(float threshold)
    {
        MIH.threshold = threshold;
    }
    public static float getThreshold()
    {
        return threshold;
    }

    public static List<String> toHash(List<float[]> features){
        List<String> result = new ArrayList<String>();
        for(int i = 0; i < features.size(); i++){
            float[] feature = features.get(i);
            String hash = toHash(feature);
            result.add(hash);
        }
        return result;
    }

    public static String toHash(float[] feature) {
        if(feature == null)
            return null;
        String hash = "";
        for(float f : feature){
            hash += (f > 0 ? "1" : "0");
        }
        return hash;
    }

    public static CompareInfo getSamePersons(List<float[]> features, List<String> ids, float[] queryFeature){
        CompareInfo compareInfo = new CompareInfo();
        if(features == null || ids == null || queryFeature == null
                || features.size() <= 0 || ids.size() <= 0)
            return null;
        //List<Boolean> result = new ArrayList<Boolean>();
        Map<String, Float> scoreMap = new HashMap<String, Float>();
        String id = null;
        for(int i = 0; i < features.size(); i++){
            float res = cosineSimilarity(features.get(i), queryFeature) - threshold;
            fileLogger.log("res", "res is " + res);
            String key = ids.get(i);
            Float value = scoreMap.get(key);
            if(value == null) value = new Float(0.0f);
            if(res > 0) {
                scoreMap.put(key, value + 0.7f * res);
                //scoreMap.put(key, value + res);
            } else {
                scoreMap.put(key, value + 0.3f * res);
                //scoreMap.put(key, value);
            }
        }

        Map.Entry<String, Float> maxEntry = null;
        for(Map.Entry<String, Float> entry : scoreMap.entrySet()){
            if(maxEntry == null || entry.getValue() > maxEntry.getValue()){
                maxEntry = entry;
            }
        }
        if(maxEntry != null && maxEntry.getValue() > 0){
            id = maxEntry.getKey();
            compareInfo.setSimilarity(maxEntry.getValue());
        }
        compareInfo.setmId(id);
        return compareInfo;
    }

    /**
     * 通过求取待比较特征与黑名单所有人脸的特征值的余弦，求得每个人余弦的平均值，最重选择平均值最大的那个人作为最相似的那个人
     * @param features 黑名单中所有人脸的特征
     * @param ids 对应于features，即每一张人脸对应的人的id
     * @param queryFeature 待比较特征
     * @return 比较信息
     */
    public static CompareInfo getSamePersonsAverage(List<float[]> features, List<String> ids, float[] queryFeature){
        CompareInfo compareInfo = new CompareInfo();
        if(features == null || ids == null || queryFeature == null
                || features.size() <= 0 || ids.size() <= 0)
            return null;
        //List<Boolean> result = new ArrayList<Boolean>();
        Map<String, Float> scoreMap = new HashMap<>();
        Map<String, Integer> countMap = new HashMap<>();

        String id = null;
        for(int i = 0; i < features.size(); i++) {
            float res = cosineSimilarity(features.get(i), queryFeature) - threshold;
            fileLogger.log("res", "res is " + res);
            String key = ids.get(i);
            Float value = scoreMap.get(key);
            Integer count = countMap.get(key);
            if(value == null) {
                value = new Float(0.0f);
            }
            if (count == null) {
                count = new Integer(0);
            }

            scoreMap.put(key, value + res);

            countMap.put(key, ++count);
        }

        Map.Entry<String, Float> maxEntry = null;
        for(Map.Entry<String, Float> entry : scoreMap.entrySet()){
            Float value = entry.getValue();
            String key = entry.getKey();
            entry.setValue(value/countMap.get(key));
            if(maxEntry == null || entry.getValue() > maxEntry.getValue()){
                maxEntry = entry;
            }
        }
        if(maxEntry != null && maxEntry.getValue() > 0){
            id = maxEntry.getKey();
            compareInfo.setSimilarity(maxEntry.getValue());
        }
        compareInfo.setmId(id);
        return compareInfo;
    }

    private static Map<String, float[]> getCenterRes(List<float[]> features, List<String> ids) {
        Map<String, float[]> centerFeature = new HashMap<>();
        Map<String, List<float[]>> personFeatures = new HashMap<>();
        for (int i = 0; i < features.size(); i++) {
            String key = ids.get(i);
            List<float[]> featureList = personFeatures.get(key);
            if (featureList == null) {
                featureList = new ArrayList<>();
            }
            featureList.add(features.get(i));
            personFeatures.put(key, featureList);
        }

        for (Map.Entry<String, List<float[]>> entry : personFeatures.entrySet()) {
            String key = entry.getKey();
            List<float[]> value = entry.getValue();

            float[] center = new float[512];
            for (int i = 0; i < 512; i++) {
                int sum = 0;
                for (float[] feature : value) {
                    sum += feature[i];
                }
                center[i] = sum/value.size();
            }
            centerFeature.put(key, center);
        }

        return centerFeature;
    }

    /**
     * 先求黑名单中每一个人特征矢量的中心点，最后得到待比较特征距离最近的中心点对应的那个人
     * @param features 黑名单中所有人脸的特征
     * @param ids 对应于features，即每一张人脸对应的人的id
     * @param queryFeature 待比较特征
     * @return 比较信息
     */
    public static CompareInfo getSamePersonsCenter(List<float[]> features, List<String> ids, float[] queryFeature){
        CompareInfo compareInfo = new CompareInfo();
        if(features == null || ids == null || queryFeature == null
                || features.size() <= 0 || ids.size() <= 0)
            return null;
        //List<Boolean> result = new ArrayList<Boolean>();
        Map<String, Float> scoreMap = new HashMap<String, Float>();
        Map<String, float[]> center = getCenterRes(features, ids);
        String id = null;

        for(Map.Entry<String, float[]> entry : center.entrySet()){
            float res = cosineSimilarity(entry.getValue(), queryFeature) - threshold;
            String key = entry.getKey();
            scoreMap.put(key, res);
        }

        Map.Entry<String, Float> maxEntry = null;
        for(Map.Entry<String, Float> entry : scoreMap.entrySet()) {
            if(maxEntry == null || entry.getValue() > maxEntry.getValue()) {
                maxEntry = entry;
            }
        }
        if(maxEntry != null && maxEntry.getValue() > 0) {
            id = maxEntry.getKey();
            compareInfo.setSimilarity(maxEntry.getValue());
        }
        compareInfo.setmId(id);
        return compareInfo;
    }

    /**
     *求top k个人中最相似的那个人
     * @param features 黑名单中所有人脸的特征
     * @param ids 对应于features，即每一张人脸对应的人的id
     * @param queryFeature 待比较特征
     * @return 比较信息
     */
    public static CompareInfo getSamePersonsTopK(List<float[]> features, List<String> ids, float[] queryFeature){
        CompareInfo compareInfo = new CompareInfo();
        if(features == null || ids == null || queryFeature == null
                || features.size() <= 0 || ids.size() <= 0)
            return null;
        //List<Boolean> result = new ArrayList<Boolean>();
        Map<String, Float> scoreMap = new HashMap<String, Float>();
        String id = null;
        for(int i = 0; i < features.size(); i++){
            float res = cosineSimilarity(features.get(i), queryFeature) - threshold;
            fileLogger.log("res", "res is " + res);
            String key = ids.get(i);
            Float value = scoreMap.get(key);
            if(value == null) value = new Float(0.0f);
            if(res > 0) {
                scoreMap.put(key, value + 0.7f * res);
                //scoreMap.put(key, value + res);
            } else{
                scoreMap.put(key, value + 0.3f * res);
                //scoreMap.put(key, value);
            }
        }


        Map.Entry<String, Float> maxEntry = null;
        for(Map.Entry<String, Float> entry : scoreMap.entrySet()){
            if(maxEntry == null || entry.getValue() > maxEntry.getValue()){
                maxEntry = entry;
            }
        }
        if(maxEntry != null && maxEntry.getValue() > 0){
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
        return (float)(0.5 + 0.5 * ( dotProduct / (Math.sqrt(normA) * Math.sqrt(normB))));
    }

}
