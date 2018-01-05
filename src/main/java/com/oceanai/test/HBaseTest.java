package com.oceanai.test;

import com.google.gson.Gson;
import com.oceanai.api.MIH;
import com.oceanai.model.CompareInfo;
import com.oceanai.model.SearchFeature;
import com.oceanai.util.FaceTool;
import com.oceanai.util.HBaseHelper;
import com.oceanai.util.ImageUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by WangRupeng on 2017/11/10.
 */
public class HBaseTest {
    private String faceInfoTable = "face_info_table";
    private String faceInfoFamily = "info";
    private String[] faceInfoColumns = {"face_id", "feature", "face_url", "hash_key", "timestamp", "person_uid"};

    private String personInfoTable = "person_info_table";
    private String personInfoFamily = "info";
    private String[] personInfoColumns = {"name", "sex", "age", "id_card", "timestamp", "face_count", "type", "state"};

    private ResultScanner results;
    private ResultScanner resultsPersonInfo;

    private Gson gson = new Gson();

    private List<float[]> features = new ArrayList<>();
    private List<String> person_ids = new ArrayList<>();
    private Map<String, String> faceBlackUrl = new HashMap<>();
    private Map<String, String> personInfo = new HashMap<>();

    private FaceTool faceTool;

    public HBaseTest() {
        try {
            faceTool = FaceTool.getInstance();

            HBaseHelper mHBaseHelper = new HBaseHelper("zk05", 2181);
            results = mHBaseHelper.getAllRows(faceInfoTable);
            resultsPersonInfo = mHBaseHelper.getAllRows(personInfoTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void search(BufferedImage bufferedImage, int number) throws Exception {
        for (Result resultFace : results) {
            String feature = Bytes.toString(resultFace.getValue(Bytes.toBytes(faceInfoFamily), Bytes.toBytes(faceInfoColumns[1])));
            String id = Bytes.toString(resultFace.getValue(Bytes.toBytes(faceInfoFamily), Bytes.toBytes(faceInfoColumns[5])));
            String face_id = Bytes.toString(resultFace.getValue(Bytes.toBytes(faceInfoFamily), Bytes.toBytes(faceInfoColumns[0])));
            String blackUrl = Bytes.toString(resultFace.getValue(Bytes.toBytes(faceInfoFamily), Bytes.toBytes(faceInfoColumns[2])));

            features.add(gson.fromJson(feature,float[].class));
            person_ids.add(id);
            faceBlackUrl.put(face_id, blackUrl);

            System.out.println("Id is " + id + " face id is " + face_id + " url is " + blackUrl);
        }

        for (Result resultFace : resultsPersonInfo) {
            String key = Bytes.toString(resultFace.getRow());
            String name_person = Bytes.toString(resultFace.getValue(Bytes.toBytes(personInfoFamily), Bytes.toBytes(personInfoColumns[0])));
            personInfo.put(key, name_person);
            System.out.println("Person name is " + name_person + " key is " + key);
        }

        List<SearchFeature> faceInfos = detectFace(bufferedImage);
        CompareInfo[] compareInfos = faceCompare(faceInfos);
        if (compareInfos == null) {
            compareInfos = new CompareInfo[0];
        }
        System.out.println("Compare info size is " + compareInfos.length);
        String[] names = new String[faceInfos.size()];
        String person_id = "";
        Graphics2D graphics2D = bufferedImage.createGraphics();
        if (faceInfos.size() > 0) {
            for (int j = 0;j<compareInfos.length;j++) {
                SearchFeature.BBox bbox = faceInfos.get(j).bbox;
                person_id = compareInfos[j].getmId();
                System.out.println("Person id is " + person_id);
                if (person_id != null && compareInfos[j].getSimilarity() > 0) {
                    names[j] = personInfo.get(person_id);
                    System.out.println("Name is " + names[j]);
                }
                Rectangle box = new Rectangle(bbox.left_top.x, bbox.left_top.y, bbox.right_down.x - bbox.left_top.x, bbox.right_down.y - bbox.left_top.y);

                //fileLogger.log(TAG, "Draw info : " + names[j] + "similarity is " + (compareInfos[j].getSimilarity() + MIH.getThreshold()));
                draw(graphics2D, box, names[j]);
            }
            if (compareInfos.length == 0) {
                for (int j = 0;j<faceInfos.size();j++) {
                    SearchFeature.BBox bbox = faceInfos.get(j).bbox;
                    Rectangle box = new Rectangle(bbox.left_top.x, bbox.left_top.y, bbox.right_down.x - bbox.left_top.x, bbox.right_down.y - bbox.left_top.y);
                    draw(graphics2D, box, names[j]);
                }
            }
            ImageUtils.saveToFile(bufferedImage, "/home/hadoop/wrp/RealtimeStreamPublish/java/faces/", "result" + number, "jpg");
        }
    }

    private List<SearchFeature> detectFace(BufferedImage bufferedImage) {
        //face detect
        int width = bufferedImage.getWidth();
        int height = bufferedImage.getHeight();
        byte[] image = ImageUtils.decodeToPixels(bufferedImage);

        FaceTool.ImageInfo imageInfo = new FaceTool.ImageInfo(image, width, height);

        List<SearchFeature> faceInfos = faceTool.extract(imageInfo);

        System.out.println("Face number " + faceInfos.size());
        return faceInfos;
    }

    private CompareInfo[] faceCompare(List<SearchFeature> faceInfos) throws Exception {
        if (features.size() == 0 || person_ids.size() == 0) {
            return null;
        }
        CompareInfo[] compareInfos = new CompareInfo[faceInfos.size()];
        int i = 0;
        for (SearchFeature info : faceInfos) {
            float[] featureFloat = info.feature;
            CompareInfo compareInfo = MIH.getSamePersons(features, person_ids, featureFloat);
            compareInfos[i++] = compareInfo;
        }
        return compareInfos;
    }

    private void draw(Graphics2D graphics2D, Rectangle box, String name) {
        if (name == null) {
            name = "";
        }
        if (name != "") {
            graphics2D.setColor(Color.RED);
            Font stringFont = new Font("楷体", Font.PLAIN, 18);
            graphics2D.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            // 计算文字长度，计算居中的x点坐标
            FontMetrics fm = graphics2D.getFontMetrics(stringFont);
            int textWidth = fm.stringWidth(name);
            int widthX = ((int)box.getWidth() - textWidth) / 2 + (int)box.getX();
            graphics2D.setFont(stringFont);
            graphics2D.drawString(name, widthX, (int) box.getY() - 5);
        } else {
            graphics2D.setColor(Color.YELLOW);
            //graphics2D.draw(box);
        }
        Point2D point2DA = new Point((int)box.getX(), (int)box.getY());
        Point2D point2DB = new Point((int)(box.getX() + box.getWidth()), (int)box.getY());
        Point2D point2DC = new Point((int)(box.getX() + box.getWidth()), (int)(box.getY() + box.getHeight()));
        Point2D point2DD = new Point((int)box.getX(), (int)(box.getY() + box.getHeight()));

        double width = box.getWidth();
        double height = box.getHeight();

        Line2D lineA_1 = new Line2D.Double(point2DA.getX(),point2DA.getY(),point2DA.getX()+width/4, point2DA.getY());
        Line2D lineA_2 = new Line2D.Double(point2DA.getX(), point2DA.getY(), point2DA.getX(), point2DA.getY() + height/4);
        Line2D lineB_1 = new Line2D.Double(point2DB.getX(), point2DB.getY(), point2DB.getX() - width/4, point2DA.getY());
        Line2D lineB_2 = new Line2D.Double(point2DB.getX(), point2DB.getY(), point2DB.getX(), point2DA.getY() + height/4);
        Line2D lineC_1 = new Line2D.Double(point2DC.getX(), point2DC.getY(), point2DC.getX(), point2DC.getY() - height/4);
        Line2D lineC_2 = new Line2D.Double(point2DC.getX(), point2DC.getY(), point2DC.getX() - width/4, point2DC.getY());
        Line2D lineD_1 = new Line2D.Double(point2DD.getX(), point2DD.getY(), point2DD.getX() + width/4, point2DD.getY());
        Line2D lineD_2 = new Line2D.Double(point2DD.getX(), point2DD.getY(), point2DD.getX(), point2DD.getY() - height/4);

        graphics2D.setStroke(new BasicStroke(1));
        graphics2D.draw(box);
        graphics2D.setStroke(new BasicStroke(4));
        graphics2D.draw(lineA_1);
        graphics2D.draw(lineA_2);
        graphics2D.draw(lineB_1);
        graphics2D.draw(lineB_2);
        graphics2D.draw(lineC_1);
        graphics2D.draw(lineC_2);
        graphics2D.draw(lineD_1);
        graphics2D.draw(lineD_2);
    }

    public static void main(String[] args) {
        try {
            HBaseTest hBaseTest = new HBaseTest();

            for (int i = 0; i < 3; i++) {
                File file = new File("/home/hadoop/wrp/RealtimeStreamPublish/java/faces/wangrupeng" + i + ".jpg");
                BufferedImage bufferedImage = ImageIO.read(file);
                hBaseTest.search(bufferedImage, i);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
