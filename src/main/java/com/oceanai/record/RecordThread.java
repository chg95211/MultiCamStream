package com.oceanai.record;

import com.oceanai.util.FileLogger;
import com.oceanai.util.ImageUtils;
import javafx.scene.image.Image;
import org.bytedeco.javacpp.avcodec;
import org.bytedeco.javacpp.avutil;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameRecorder;
import org.bytedeco.javacv.Java2DFrameConverter;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by WangRupeng on 2017/12/2.
 */
public class RecordThread implements Runnable {
    private static final String TAG = "RecordThread";
    private FileLogger fileLogger = new FileLogger("/home/hadoop/realtime_recorder_logs/RecordThread.log");
    private boolean running = false;
    private LinkedBlockingQueue<BufferedImage> frameQueue2;

    private FFmpegFrameRecorder recorder;
    private Java2DFrameConverter converter;
    private String destLocation = "";
    private int bitrate = 40;
    //private BufferedImage bufferedImage;
    //private Frame ff;

    public RecordThread(LinkedBlockingQueue<BufferedImage> frameQueue2, int bitrate) {
        this.frameQueue2 = frameQueue2;
        converter = new Java2DFrameConverter();
        this.bitrate = bitrate;
        try {
            //bufferedImage = ImageIO.read(new File("E:\\HUST\\MultiCamsStream\\out\\4.jpg"));
            //ff = converter.convert(bufferedImage);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start(String location) throws Exception {
        fileLogger.log(TAG, "Recorder ready to start! Bitrate is " + this.bitrate);
        if (recorder != null ) {
            //recorder.stop();
            //recorder.release();
            recorder = null;
        }
        this.destLocation = location;
        recorder = new FFmpegFrameRecorder(this.destLocation, 1280, 720);
        recorder.setFormat("rtp");
        recorder.setFrameRate(25);
        recorder.setVideoBitrate(this.bitrate * 10000);
        //recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
        recorder.setVideoCodec(avcodec.AV_CODEC_ID_MPEG2VIDEO);
        recorder.setVideoOption("tune", "zerolatency");
        recorder.setVideoOption("preset", "ultrafast"); //加快h264的编码速度

        try {
            recorder.start();
            System.out.println("Recoder started!Location is " + location);
            fileLogger.log(TAG, "Recorder started!Locatin is " + location);
        } catch (FrameRecorder.Exception e) {
            //e.printStackTrace();
        }

        running = true;
    }

    @Override
    public void run() {
        try {
            //BufferedImage testBufferedImage = ImageIO.read(new File("/home/hadoop/wrp/RealtimeStreamPublish/java/images/10.jpg"));
            while (running) {
                long start = System.currentTimeMillis();
                BufferedImage bufferedImage;
                /*if (frameQueue2.size() == 0) {
                    bufferedImage = testBufferedImage;
                } else {*/
                    bufferedImage = frameQueue2.poll();
                //}
                if (frameQueue2.size() > 90) {
                    continue;
                }
                if (bufferedImage != null && recorder != null) {
                    Frame ff = converter.convert(bufferedImage);
                    //bufferedImage = converter.convert(ff);
                    //ImageUtils.saveToFile(bufferedImage, "/home/hadoop/wrp/RealtimeStreamPublish/java/images/", "" + count++, "jpg");
                    /*long videoTS = 1000 * (System.currentTimeMillis() - startTime);
                    recorder.setTimestamp(videoTS);*/
                    recorder.record(ff);
                    System.out.println("Record one frame, record queue size is " + frameQueue2.size() + " time used " + (System.currentTimeMillis() - start));
                    fileLogger.log(TAG, "Record one frame, record queue size is " + frameQueue2.size() + " time used " + (System.currentTimeMillis() - start));
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
        running = false;
    }

    public void stop() {
        running = false;
        System.out.println("Recorder stoped!");
        fileLogger.log(TAG, "Recorder stoped!");
    }

    public boolean setLocation(String location, String port) {
        /*if (running && this.destLocation.equals(location)) {
            return true;
        }*/
        if (running) {
            this.stop();
        }
        String rtpAddress = "rtp://" + location + ":" + port;
        System.out.println("RTP address is " + rtpAddress);
        fileLogger.log(TAG, "Set RTP address " + rtpAddress);
        try {
            this.start(rtpAddress);
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return true;
    }

    public boolean isRunning() {
        return running;
    }
}
