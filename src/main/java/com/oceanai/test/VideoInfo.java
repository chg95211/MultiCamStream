package com.oceanai.test;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;

/**
 * Created by WangRupeng on 2017/11/22.
 */
public class VideoInfo {
    private FFmpegFrameGrabber grabber;
    public VideoInfo(String location) {
        grabber = new FFmpegFrameGrabber(location);
        try {
            grabber.start();
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }
    }

    public void getVideoInfo() {
        if (grabber != null) {
            int width = grabber.getImageWidth();
            int height = grabber.getImageHeight();
            int bitRate = grabber.getVideoBitrate();
            int frameLength = grabber.getLengthInFrames();
            System.out.println("Height is " + height + ", width is " + width + ", bit rate is " + bitRate + ", frame length is " + frameLength);
        }
    }

    public void close() {
        if (grabber != null) {
            try {
                grabber.stop();
            } catch (FrameGrabber.Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        VideoInfo videoInfo = new VideoInfo("G:\\QQFiles\\dji.mp4");
        videoInfo.getVideoInfo();
        videoInfo.close();
        System.out.println("Time used " + (System.currentTimeMillis() - start));
    }
}
