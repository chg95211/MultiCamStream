package com.oceanai.grab;

import com.oceanai.record.RecordThread;
import com.oceanai.util.FileLogger;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by WangRupeng on 2017/11/8.
 */
public class GrabFaceTrackerThread implements Runnable {
    private static final String TAG = "GrabFaceTrackerThread";
    //Grabber
    private FFmpegFrameGrabber grabber;
    private boolean running = false;
    private String location = "";
    private String streamID = "";

    //File logger
    private FileLogger fileLogger;
    //private Logger logger = LoggerFactory.getLogger(GrabFaceTrackerThread.class);

    //converter
    private Java2DFrameConverter mImageConverter;
    private RecordThread recordThread ;
    //
    //protected List<Frame> frameList = new ArrayList<>();
    private LinkedBlockingQueue<BufferedImage> frameList;

    public GrabFaceTrackerThread(LinkedBlockingQueue<BufferedImage> frameList) {
        fileLogger = new FileLogger("/home/hadoop/realtime_recorder_logs/GrabFaceTrackerThread.log");
        //fileLogger = new FileLogger("E:\\HUST\\HUST-BitData\\Github\\RealtimeStrreamPublish\\log\\GrabFaceTrackerThread.log");
        //faceTool = new FaceTool("/home/hadoop/storm-projects/lib/config/search.json", fileLogger);
        this.frameList = frameList;
        mImageConverter = new Java2DFrameConverter();
        //stringFont = new Font("楷体", Font.PLAIN, 18);
    }

    public GrabFaceTrackerThread(String location) {
        this.location = location;
        this.streamID = getCameraID(location);
        grabber = new FFmpegFrameGrabber(location);
        grabber.setOption("rtsp_transport", "tcp");
        grabber.setFormat("flv");
        try {
            grabber.start();
        } catch (FrameGrabber.Exception e) {
            //e.printStackTrace();
            System.out.println(e.getMessage());

        }
    }

    public void start(String location) {
        if (grabber != null) {
            grabber = null;
        }
        this.location = location;
        this.streamID = getCameraID(location);
        grabber = new FFmpegFrameGrabber(location);
        grabber.setOption("rtsp_transport", "tcp");
        try {
            grabber.start();
            /*if (recorder == null) {
                recorder = recorderInit("rtp://192.168.1.104:80", grabber.getFrameRate(), grabber.getImageWidth(), grabber.getImageHeight(), grabber.getVideoBitrate());
                recorder.start();
            }*/
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println(e.getMessage());
        }

        running = true;
    }

    @Override
    public void run() {
        try {
            System.out.println("Grab thread start!");
            System.out.println("Video stream width is " + grabber.getImageWidth() + " height is " + grabber.getImageHeight() + " bitrate is " + grabber.getVideoBitrate());
            while (running) {
                Frame frame = grabber.grabImage();
                if (frame != null) {
                    BufferedImage bufferedImage = new BufferedImage(1280, 720, BufferedImage.TYPE_3BYTE_BGR);
                    Graphics graphics = bufferedImage.getGraphics();
                    graphics.drawImage(mImageConverter.getBufferedImage(frame), 0, 0, 1280, 720, null);

                    if (bufferedImage != null && frameList.size() < 20) {
                        if (frameList.size() == 100) {
                            continue;
                        }
                        frameList.put(bufferedImage);
                        System.out.println("Grab Frame List size is " + frameList.size());
                        fileLogger.log(TAG, "Grab Frame List size is " + frameList.size());
                        /*if (frameList.size() >= 100) {
                            Utils.sleep(frameList.size());
                            //frameList.clear();
                        }*/
                    }
                    //System.out.println("StreamID is " + streamID + " frame number is " + grabber.getFrameNumber());
                    fileLogger.log(TAG, "StreamID is " + streamID + " frame number is " + grabber.getFrameNumber());
                }
            }
            System.out.println("end");

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    public boolean setLocation(String location) {
        if (running) {
            this.stop();
        }

        this.start(location);
        return true;
    }

    private String getCameraID(String location) {
        //only local ip url
        String regEx = "192.168.1.\\d+";
        Pattern pattern = Pattern.compile(regEx);
        Matcher matcher = pattern.matcher(location);
        String result = "";
        if (matcher.find()) {
            System.out.println(matcher.group(0));
            result = matcher.group(0);
            result = result.substring(result.length() - 2, result.length());
        } else {
            if(location.contains("/")) {
                result = location.hashCode() + "_" + location.substring(location.lastIndexOf("/") + 1);
            }
        }
        return result;
    }

    public void stop() {
        try {
            running = false;
            //grabber.stop();
            //grabber.release();
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}
