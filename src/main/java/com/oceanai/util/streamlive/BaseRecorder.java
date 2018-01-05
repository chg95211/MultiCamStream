package com.oceanai.util.streamlive;

import org.bytedeco.javacpp.avcodec;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.FrameRecorder;
import org.bytedeco.javacv.Java2DFrameConverter;

import java.awt.image.BufferedImage;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * Created by WangRupeng on 2017/9/25.
 */
public class BaseRecorder extends Thread {

    private LinkedBlockingQueue<BufferedImage> frameQueue2;

    private FFmpegFrameRecorder recorder;
    private Java2DFrameConverter converter;

    public BaseRecorder(LinkedBlockingQueue<BufferedImage> frameQueue2, String destAddress) {

        this.frameQueue2 = frameQueue2;

        converter = new Java2DFrameConverter();

        recorder = new FFmpegFrameRecorder(destAddress, 1280, 720);
        recorder.setFormat("rtp");
        recorder.setFrameRate(25);
        recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
        //recorder.setVideoCodec(avcodec.AV_CODEC_ID_MPEG2VIDEO);
        //recorder.setVideoBitrate(0);
        //recorder.setVideoQuality(0);
        recorder.setVideoOption("tune", "zerolatency");
//        recorder.setVideoOption("preset", "veryslow");

        try {
            recorder.start();
        } catch (FrameRecorder.Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                BufferedImage bufferedImage = frameQueue2.take();

                //ImageUtils.saveToFile(bufferedImage, "/home/hadoop/wrp/RealtimeStreamPublish/java/images/", "" + count++, "jpg");
                org.bytedeco.javacv.Frame ff = converter.convert(bufferedImage);

                long videoTS = 1000 * (System.currentTimeMillis() - startTime);
                recorder.setTimestamp(videoTS);

                recorder.record(ff);
            }  catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}