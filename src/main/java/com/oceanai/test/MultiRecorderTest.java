package com.oceanai.test;

import com.oceanai.util.ImageUtils;
import org.bytedeco.javacpp.avcodec;
import org.bytedeco.javacpp.presets.opencv_core;
import org.bytedeco.javacv.*;

import java.awt.image.BufferedImage;

/**
 * Created by WangRupeng on 2017/11/21.
 */
public class MultiRecorderTest {
    private FFmpegFrameGrabber grabber;
    private FFmpegFrameRecorder recorder;
    private FFmpegFrameRecorder recorder2;
    private int frameLength = 0;
    private int frameNumber = 0;
    private String streamLocation = "";

    private long startTime = System.currentTimeMillis();

    private Java2DFrameConverter converter;

    public MultiRecorderTest(String grabLocation, String recordLocation, String recordLocation2) {
        initGrabber(grabLocation);
        streamLocation = grabLocation;
        converter = new Java2DFrameConverter();
        if (this.grabber != null) {
            initRecorder(recordLocation, this.grabber.getImageWidth(), this.grabber.getImageHeight(), this.grabber.getVideoBitrate());
            //initRecorder2(recordLocation2, this.grabber.getImageWidth(), this.grabber.getImageHeight());
        } else {
            System.out.println("Grabber hasn't been init!");
        }
    }

    private void initGrabber(String location) {
        grabber = new FFmpegFrameGrabber(location);
        try {
            grabber.start();
            frameLength = grabber.getLengthInFrames();

        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }
    }

    private void initRecorder(String dstLocation, int width, int height, int bitrate) {
        recorder = new FFmpegFrameRecorder(dstLocation, width, height);
        recorder.setFormat("rtp");
        recorder.setFrameRate(25);
        recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
        //recorder.setVideoCodec(avcodec.AV_CODEC_ID_MPEG2VIDEO);
        recorder.setVideoOption("tune", "zerolatency"); // or ultrafast or fast, etc.
        recorder.setVideoBitrate(bitrate);
        try {
            recorder.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initRecorder2(String dstLocation, int width, int height) {
        recorder2 = new FFmpegFrameRecorder(dstLocation, width, height);
        recorder2.setFormat("rtp");
        recorder2.setFrameRate(25);
        recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
        //recorder2.setVideoCodec(avcodec.AV_CODEC_ID_MPEG2VIDEO);
        recorder2.setVideoOption("tune", "zerolatency"); // or ultrafast or fast, etc.
        try {
            recorder2.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public void record(Frame frame) {
        try {
            if (recorder != null) {
                long videoTS = 1000 * (System.currentTimeMillis() - startTime);
                recorder.setTimestamp(videoTS);
                recorder.record(frame);
                System.out.println("Record one frame!");
            } else {
                System.out.println("Recorder hasn't been init!");
            }
        }catch (FFmpegFrameRecorder.Exception e) {
            e.printStackTrace();
        }
    }

    public void grab() {
        while (frameNumber < frameLength ) {
            try {
                if (frameNumber >= frameLength - 2) {
                    grabber.stop();
                    grabber.release();
                    grabber = null;
                    grabber = new FFmpegFrameGrabber(streamLocation);
                    grabber.start();
                }
                Frame frame = grabber.grabImage();
                frameNumber = grabber.getFrameNumber();
                //BufferedImage bufferedImage = converter.convert(frame);
                //ImageUtils.saveToFile(bufferedImage, "E:\\HUST\\MultiCamsStream\\out\\images\\", "" + frameNumber, "jpg");

                long videoTS = 1000 * (System.currentTimeMillis() - startTime);
                recorder.setTimestamp(videoTS);

                //record(frame);
                recorder.record(frame);
                //recorder2.record(frame);
                System.out.println("Record one frame " + frameNumber + ", frame length is " + frameLength);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public FFmpegFrameRecorder recorderFactory(String dstLocation, int width, int height) {
        FFmpegFrameRecorder recorderTemp = new FFmpegFrameRecorder(dstLocation, width, height);
        recorderTemp.setFormat("rtp");
        recorderTemp.setFrameRate(25);
        recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
        //recorderTemp.setVideoCodec(avcodec.AV_CODEC_ID_MPEG2VIDEO);
        recorderTemp.setVideoOption("tune", "zerolatency"); // or ultrafast or fast, etc.
        return recorderTemp;
    }

    public static void main(String[] args) {
        MultiRecorderTest test = new MultiRecorderTest("G:\\QQFiles\\dji.mp4",
                "rtp://192.168.1.103:80", "rtp://127.0.0.1:8080");
        test.grab();

    }
}
