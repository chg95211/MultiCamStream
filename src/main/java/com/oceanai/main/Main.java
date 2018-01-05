package com.oceanai.main;

import com.oceanai.util.streamlive.BaseGrabber;
import com.oceanai.util.streamlive.BaseRecorder;
import com.oceanai.util.streamlive.Extractor;
import com.oceanai.util.FaceTool;
import org.bytedeco.javacpp.avutil;

import java.awt.image.BufferedImage;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Main start
 * Created by Wangke on 2017/9/25.
 */
public class Main {

    public static final String realtimeFacesTable = "realtime_faces_table";
    public static final String realtimeFacesFamily = "info";
    public static final String[] realtimeFacesColumns = {"feature", "face_url", "origin_url", "hashkey", "person_uid", "camera_id", "timestamp"};

    public static final String hadoopUrl = "hdfs://cloud06:8020/user/hadoop/realtime/";
    public static final String hadoopUrl_face = "hdfs://cloud06:8020/user/hadoop/realtime_faces/";

    public static final String ClipRoot = "/home/hadoop/";
    public static boolean recording = true;

    private static LinkedBlockingQueue<BufferedImage> frameQueue1 = new LinkedBlockingQueue<>(); //base grabber
    private static LinkedBlockingQueue<BufferedImage> frameQueue2 = new LinkedBlockingQueue<>(); //base recorder
    private static LinkedBlockingQueue<BufferedImage> frameQueue3 = new LinkedBlockingQueue<>(); //branch grabber

    public static void main(String[] args) {

        //String srcAddress = "E:\\HUST\\HUST-BitData\\Github\\RealtimeStrreamPublish\\video\\test20170523.mp4";
        //String srcAddress = "/home/hadoop/wrp/RealtimeStreamPublish/java/video/lab.mp4";
        String srcAddress = "rtsp://admin:iec123456@192.168.1.59:554/h264/ch1/main/av_stream";
//        String destAddress = "rtp://192.168.1.7:80";
        String destAddress = "rtp://192.168.1.104:80";

        Map<String, String> map = new HashMap<>();
        map.put("31", "rtsp://admin:123456@192.168.1.31:554/unicast/c1/s0/live");
//        map.put("32", "rtsp://admin:123456@192.168.1.32:554/unicast/c1/s0/live");
        map.put("33", "rtsp://admin:123456@192.168.1.33:554/unicast/c1/s0/live");
        map.put("34", "rtsp://admin:123456@192.168.1.34:554/unicast/c1/s0/live");
        map.put("36", "rtsp://admin:123456@192.168.1.36:554/unicast/c1/s0/live");
        map.put("38", "rtsp://admin:123456@192.168.1.38:554/unicast/c1/s0/live");
//        map.put("43", "rtsp://admin:123456@192.168.1.43:554/unicast/c1/s0/live");
//        map.put("61", "rtsp://admin:123456@192.168.1.61:554/unicast/c1/s0/live");
//        map.put("127", "rtsp://admin:123456@192.168.1.127:554/unicast/c1/s0/live");

        new Thread(() -> {
            //HBaseHelper.getInstance();
            //HDFSHelper.getInstance();
            FaceTool.getInstance();
        }).start();

        avutil.av_log_set_level(16);

        /************************ stream ************************/
        BaseGrabber bg = new BaseGrabber(frameQueue1, srcAddress);
        Extractor ex = new Extractor(frameQueue1, frameQueue2);
        BaseRecorder br = new BaseRecorder(frameQueue2, destAddress);

        bg.start();
        ex.start();
        br.start();

        /************************ capture ************************/
//        for (Map.Entry<String, String> entry : map.entrySet()) {
//            BranchGrabber g = new BranchGrabber(frameQueue3, entry);
//            g.start();
//        }
//
//        Extractor2 a = new Extractor2(frameQueue3);
//
////        a.setPriority(10);
//        a.start();

        /************************ record ************************/
        /*Map<Runnable, Thread> threadMap = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            Record recorder = new Record(frameQueue3, entry);
            Thread thread = new Thread(recorder);
            threadMap.put(recorder, thread);

            thread.start();
        }

        Monitor monitor = new Monitor(threadMap);
        monitor.start();

        *//************************ capture2 ************************//*
        Extractor2 a = new Extractor2(frameQueue3);

//        a.setPriority(10);
        a.start();*/
    }
}
