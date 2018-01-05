package com.oceanai.util.streamlive;

import boofcv.abst.tracker.TrackerObjectQuad;
import boofcv.factory.tracker.FactoryTrackerObjectQuad;
import boofcv.io.image.ConvertBufferedImage;
import boofcv.struct.image.GrayU8;
import boofcv.struct.image.ImageBase;
import boofcv.struct.image.ImageType;
import com.google.gson.Gson;
import com.oceanai.api.MIH;
import com.oceanai.model.CompareInfo;
import com.oceanai.model.Frame;
import com.oceanai.model.SearchFeature;
import com.oceanai.util.*;
import georegression.struct.shapes.Quadrilateral_F64;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.awt.*;
import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Stream implements Serializable {
    private static final long serialVersionUID = -9044320523670966102L;
    private static String TAG = "Stream";
    private List<SearchFeature> faceInfos;

    private TrackerObjectQuad[] trackers = null;
    private Quadrilateral_F64[] locations = null;
    private ImageType<GrayU8> imageType = null;
    private ImageBase imageBase = null;
    private GrayU8 currentBoof = null;

    int number = 0;

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
    private String[] names;

    private FaceTool faceTool;

    private FileLogger fileLogger = new FileLogger("/home/hadoop/realtime_recorder_logs/" + TAG + ".log");

    private BASE64Encoder encoder = new BASE64Encoder();
    @SuppressWarnings("unchecked")
    public Stream() throws Exception {
        //Face detect and extract
        //faceTool = FaceTool.getInstance();
        faceTool = new FaceTool();
        fileLogger.log(TAG, "Face tool init finished! Threshold is " + MIH.getThreshold());
        faceInfos = new ArrayList<>();
        //Tracker init
        TrackerObjectQuad tracker = FactoryTrackerObjectQuad.circulant(null, GrayU8.class);
        imageType = tracker.getImageType();

        //HBase
        System.out.println("Start to init HBase");
        HBaseHelper mHBaseHelper = new HBaseHelper("cloud04", 2181);
        this.results = mHBaseHelper.getAllRows(faceInfoTable);
        this.resultsPersonInfo = mHBaseHelper.getAllRows(personInfoTable);
        fileLogger.log(TAG, "HBase init finished!");
        //HBase face and person info
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
    }

    @SuppressWarnings("unchecked")
    public BufferedImage execute(BufferedImage bufferedImage) throws Exception {
        if (bufferedImage == null) {
            return null;
        }
        BufferedImage currentImage = new BufferedImage(bufferedImage.getWidth(), bufferedImage.getHeight(),
                BufferedImage.TYPE_3BYTE_BGR);
        Graphics2D graphics2D = bufferedImage.createGraphics();

        //drawPre(graphics2D, Main.recording);

        if (number++ % 10 == 0) {
            long now = System.currentTimeMillis();
            //FaceTool.ImageInfo imageInfo = new FaceTool.ImageInfo(bufferedImage);
            //faceInfos = faceTool.extract(imageInfo);
            //System.out.println();
            fileLogger.log(TAG, "Face api server is " + HttpClientUtil.getUri());
            faceInfos = faceTool.extract(encoder.encode(ImageUtils.encodeToImage(bufferedImage, "jpg")));
            fileLogger.log(TAG, "Face number is " + faceInfos.size());
            System.out.println("*****************detectFace consumes " + (System.currentTimeMillis() - now) + "ms");
            names = new String[faceInfos.size()];
            now = System.currentTimeMillis();
            CompareInfo[] compareInfos = faceCompare(faceInfos);
            if (compareInfos == null) {
                compareInfos = new CompareInfo[faceInfos.size()];
            }
            fileLogger.log(TAG, "Compare info number is " + compareInfos.length);
            System.out.println("*****************compare faces consumes " + (System.currentTimeMillis() - now) + "ms");

            if (faceInfos.size() > 0) {
                System.out.println("Contains face, Face number is " + faceInfos.size());
                fileLogger.log(TAG, "Contains face, Face number is " + faceInfos.size());
                faceTrackingInit(bufferedImage);
                for (int j = 0;j < faceInfos.size();j++) {
                    SearchFeature.BBox bbox = faceInfos.get(j).bbox;
                    String person_id = compareInfos[j].getmId();
                    if (compareInfos[j].getSimilarity() > 0) {
                        //names[j] = students.get(faceMap.get(face_id));
                        names[j] = personInfo.get(person_id);
                    } else {
                        names[j] = "";
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
            }
        } else {
            if (faceInfos.size() > 0) {
                long now = System.currentTimeMillis();
                imageBase = convert(currentImage, currentBoof, bufferedImage);
                for (int n = 0; n < faceInfos.size(); n++) {
                    trackers[n].process(imageBase, locations[n]);
                    Rectangle box = new Rectangle((int) locations[n].getA().getX(), (int) locations[n].getA()
                            .getY(), (int) (locations[n].getC().getX() - locations[n].getA().getX()), (int)
                            (locations[n].getC().getY() - locations[n].getA().getY()));

                    draw(graphics2D, box, names[n]);
                }
                System.out.println("Track one frame time used " + (System.currentTimeMillis() - now));
            }
        }

        graphics2D.dispose();
        return bufferedImage;
    }

    public Frame executeBuff(Frame input) throws Exception {
        BufferedImage bufferedImage = input.getBoxedBufferedImage();
        BufferedImage currentImage = new BufferedImage(bufferedImage.getWidth(), bufferedImage.getHeight(),
                BufferedImage.TYPE_3BYTE_BGR);
        //byte[] image = bufferedImage.getImageBytes();

        Graphics2D graphics2D = bufferedImage.createGraphics();

        //drawPre(graphics2D, Main.recording);

        if (number++ % 5 == 0) {

            long now = System.currentTimeMillis();

            FaceTool.ImageInfo imageInfo = new FaceTool.ImageInfo(bufferedImage);
            faceInfos = faceTool.extract(imageInfo);
            System.out.println("*****************detectFace consumes " + (System.currentTimeMillis() - now) + "ms");

            if (faceInfos.size() > 0) {
                faceTrackingInit(bufferedImage);

                for (SearchFeature faceInfo : faceInfos) {
                    SearchFeature.BBox bbox = faceInfo.bbox;
                    Rectangle box = new Rectangle(bbox.left_top.x, bbox.left_top.y, bbox.right_down.x - bbox
                            .left_top.x, bbox.right_down.y - bbox.left_top.y);

                    draw(graphics2D, box);
                }
            }
        } else {
            if (faceInfos.size() > 0) {
                imageBase = convert(currentImage, currentBoof, bufferedImage);
                for (int n = 0; n < faceInfos.size(); n++) {
                    trackers[n].process(imageBase, locations[n]);
                    Rectangle box = new Rectangle((int) locations[n].getA().getX(), (int) locations[n].getA()
                            .getY(), (int) (locations[n].getC().getX() - locations[n].getA().getX()), (int)
                            (locations[n].getC().getY() - locations[n].getA().getY()));

                    draw(graphics2D, box);
                }
            }
            return input;
        }
        input.setBoxedBufferedImage(bufferedImage);
        graphics2D.dispose();

        return input;

    }

    public BufferedImage executeBuffWithoutTrack(BufferedImage bufferedImage) throws Exception {
        BufferedImage currentImage = new BufferedImage(bufferedImage.getWidth(), bufferedImage.getHeight(),
                BufferedImage.TYPE_3BYTE_BGR);
        //byte[] image = bufferedImage.getImageBytes();

        Graphics2D graphics2D = bufferedImage.createGraphics();

        //drawPre(graphics2D, Main.recording);

        if (number++ % 5 == 0) {
            long now = System.currentTimeMillis();
            FaceTool.ImageInfo imageInfo = new FaceTool.ImageInfo(bufferedImage);
            faceInfos = faceTool.extract(imageInfo);
            System.out.println("*****************detectFace consumes " + (System.currentTimeMillis() - now) + "ms");

            if (faceInfos.size() > 0) {
                faceTrackingInit(bufferedImage);

                for (SearchFeature faceInfo : faceInfos) {
                    SearchFeature.BBox bbox = faceInfo.bbox;
                    Rectangle box = new Rectangle(bbox.left_top.x, bbox.left_top.y, bbox.right_down.x - bbox
                            .left_top.x, bbox.right_down.y - bbox.left_top.y);

                    draw(graphics2D, box);
                }
            }
        } else {
            if (faceInfos.size() > 0) {
                imageBase = convert(currentImage, currentBoof, bufferedImage);
                for (int n = 0; n < faceInfos.size(); n++) {
                    trackers[n].process(imageBase, locations[n]);
                    Rectangle box = new Rectangle((int) locations[n].getA().getX(), (int) locations[n].getA()
                            .getY(), (int) (locations[n].getC().getX() - locations[n].getA().getX()), (int)
                            (locations[n].getC().getY() - locations[n].getA().getY()));

                    draw(graphics2D, box);
                }
            }
        }
        graphics2D.dispose();

        return bufferedImage;

    }

    @SuppressWarnings("unchecked")
    private void faceTrackingInit(BufferedImage bufferedImage) {
        BufferedImage currentImage = new BufferedImage(bufferedImage.getWidth(), bufferedImage.getHeight(), BufferedImage
                .TYPE_3BYTE_BGR);
        currentBoof = imageType.createImage(bufferedImage.getWidth(), bufferedImage.getHeight());
        imageBase = convert(currentImage, currentBoof, bufferedImage);

        locations = new Quadrilateral_F64[faceInfos.size()];
        trackers = new TrackerObjectQuad[faceInfos.size()];
        for (int i = 0; i < faceInfos.size(); i++) {
            trackers[i] = FactoryTrackerObjectQuad.circulant(null, GrayU8.class);
            SearchFeature.BBox bbox = faceInfos.get(i).bbox;
            locations[i] = new Quadrilateral_F64(bbox.left_top.x, bbox.left_top.y, bbox.right_down.x, bbox.left_top
                    .y, bbox.right_down.x, bbox.right_down.y, bbox.left_top.x, bbox.right_down.y);
            trackers[i].initialize(imageBase, locations[i]);
        }
    }

    private void drawPre(Graphics2D graphics2D, boolean recording) {

        Line2D lineE_1 = new Line2D.Double(540, 240, 740, 240);
        Line2D lineE_2 = new Line2D.Double(520, 260, 520, 460);
        Line2D lineF_1 = new Line2D.Double(740, 480, 540, 480);
        Line2D lineF_2 = new Line2D.Double(760, 460, 760, 260);

        Line2D line1 = new Line2D.Double(540, 250, 570, 250);
        Line2D line2 = new Line2D.Double(740, 250, 710, 250);
        Line2D line3 = new Line2D.Double(530, 260, 530, 290);
        Line2D line4 = new Line2D.Double(750, 260, 750, 290);

        Line2D line5 = new Line2D.Double(530, 460, 530, 430);
        Line2D line6 = new Line2D.Double(540, 470, 570, 470);
        Line2D line7 = new Line2D.Double(740, 470, 710, 470);
        Line2D line8 = new Line2D.Double(750, 460, 750, 430);

        graphics2D.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        graphics2D.setStroke(new BasicStroke(3));
        graphics2D.setColor(recording ? Color.WHITE : Color.GREEN);

        graphics2D.draw(lineE_1);
        graphics2D.draw(lineE_2);
        graphics2D.draw(lineF_1);
        graphics2D.draw(lineF_2);

        graphics2D.draw(line1);
        graphics2D.draw(line2);
        graphics2D.draw(line3);
        graphics2D.draw(line4);
        graphics2D.draw(line5);
        graphics2D.draw(line6);
        graphics2D.draw(line7);
        graphics2D.draw(line8);

        graphics2D.drawArc(520, 240, 40, 40, 90, 90);
        graphics2D.drawArc(720, 240, 40, 40, 0, 90);
        graphics2D.drawArc(520, 440, 40, 40, 180, 90);
        graphics2D.drawArc(720, 440, 40, 40, 270, 90);

        graphics2D.drawArc(530, 250, 20, 20, 90, 90);
        graphics2D.drawArc(730, 250, 20, 20, 0, 90);
        graphics2D.drawArc(530, 450, 20, 20, 180, 90);
        graphics2D.drawArc(730, 450, 20, 20, 270, 90);
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
            //CompareInfo compareInfo = MIH.getSamePersonsAverage(features, person_ids, featureFloat);
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

    private void draw(Graphics2D graphics2D, Rectangle box) {

        graphics2D.setColor(Color.YELLOW);

        Point2D point2DA = new Point((int) box.getX(), (int) box.getY());
        Point2D point2DB = new Point((int) (box.getX() + box.getWidth()), (int) box.getY());
        Point2D point2DC = new Point((int) (box.getX() + box.getWidth()), (int) (box.getY() + box.getHeight()));
        Point2D point2DD = new Point((int) box.getX(), (int) (box.getY() + box.getHeight()));

        double width = box.getWidth();
        double height = box.getHeight();

        Line2D lineA_1 = new Line2D.Double(point2DA.getX(), point2DA.getY(), point2DA.getX() + width / 4, point2DA.getY());
        Line2D lineA_2 = new Line2D.Double(point2DA.getX(), point2DA.getY(), point2DA.getX(), point2DA.getY() + height / 4);
        Line2D lineB_1 = new Line2D.Double(point2DB.getX(), point2DB.getY(), point2DB.getX() - width / 4, point2DA.getY());
        Line2D lineB_2 = new Line2D.Double(point2DB.getX(), point2DB.getY(), point2DB.getX(), point2DA.getY() + height / 4);
        Line2D lineC_1 = new Line2D.Double(point2DC.getX(), point2DC.getY(), point2DC.getX(), point2DC.getY() - height / 4);
        Line2D lineC_2 = new Line2D.Double(point2DC.getX(), point2DC.getY(), point2DC.getX() - width / 4, point2DC.getY());
        Line2D lineD_1 = new Line2D.Double(point2DD.getX(), point2DD.getY(), point2DD.getX() + width / 4, point2DD.getY());
        Line2D lineD_2 = new Line2D.Double(point2DD.getX(), point2DD.getY(), point2DD.getX(), point2DD.getY() - height / 4);

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

    private GrayU8 convert(BufferedImage currentImage, GrayU8 currentBoof, BufferedImage next) {
        currentImage.createGraphics().drawImage(next, 0, 0, null);
        ConvertBufferedImage.convertFrom(currentImage, currentBoof, true);
        return currentBoof;
    }
}
