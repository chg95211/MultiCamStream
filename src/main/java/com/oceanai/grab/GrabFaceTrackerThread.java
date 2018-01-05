package com.oceanai.grab;

import com.oceanai.record.RecordThread;
import com.oceanai.util.FileLogger;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
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

    //converter
    private Java2DFrameConverter mImageConverter;
    private LinkedBlockingQueue<BufferedImage> frameList;

    private  BufferedImage testBufferedImage = null;

    public GrabFaceTrackerThread(LinkedBlockingQueue<BufferedImage> frameList, String logDir) {
        if (!logDir.endsWith("/")) {
            logDir = logDir + "/";
        }
        fileLogger = new FileLogger(logDir + "GrabFaceTrackerThread.log");
        this.frameList = frameList;
        mImageConverter = new Java2DFrameConverter();
        try {
            testBufferedImage = ImageIO.read(new File("/home/hadoop/wrp/RealtimeStreamPublish/java/images/loading.jpg"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
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
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        running = true;
    }

    @Override
    public void run() {
            System.out.println("Grab thread start!");
            System.out.println("Video stream width is " + grabber.getImageWidth() + " height is " + grabber.getImageHeight() + " bitrate is " + grabber.getVideoBitrate());
            while (running) {
                Frame frame = null;
                try {
                 frame = grabber.grabImage();
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    if (testBufferedImage != null) {
                        frame = mImageConverter.convert(testBufferedImage);
                    }
                }
                if (frame != null) {
                    BufferedImage bufferedImage = new BufferedImage(1280, 720, BufferedImage.TYPE_3BYTE_BGR);
                    Graphics graphics = bufferedImage.getGraphics();
                    graphics.drawImage(mImageConverter.getBufferedImage(frame), 0, 0, 1280, 720, null);

                    if (bufferedImage != null && frameList.size() < 20) {
                        if (frameList.size() == 100) {
                            continue;
                        }
                        try {
                            frameList.put(bufferedImage);
                        } catch (InterruptedException e) {
                            System.out.println(e.getMessage());
                        }
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
