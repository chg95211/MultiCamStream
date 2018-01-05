package com.oceanai.util.streamlive;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.Java2DFrameConverter;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by Wangke on 2017/9/25.
 */
public class BaseGrabber extends Thread {

    private LinkedBlockingQueue<BufferedImage> frameQueue;

    private FFmpegFrameGrabber grabber;
    private Java2DFrameConverter converter;

    private int lastNumber = -1;

    public BaseGrabber(LinkedBlockingQueue<BufferedImage> frameQueue, String baseAddress) {

        this.frameQueue = frameQueue;

        try {
            grabber = new FFmpegFrameGrabber(baseAddress);
            grabber.setOption("rtsp_transport", "tcp");
            grabber.setFrameRate(25);

            try {
                grabber.start();
            } catch (FrameGrabber.Exception e) {
                e.printStackTrace();
            }

            converter = new Java2DFrameConverter();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {

        while (true) {
            try {
                org.bytedeco.javacv.Frame frame = grabber.grabImage();
                if (frame == null) {
                    continue;
                }

                int frameNumber = grabber.getFrameNumber();
                if (frameNumber == lastNumber) {
                    continue;
                }

                lastNumber = frameNumber;

                BufferedImage bi = new BufferedImage(568, 320, BufferedImage.TYPE_3BYTE_BGR);
                //BufferedImage bi_copy = new BufferedImage(1280, 720, BufferedImage.TYPE_3BYTE_BGR);

                Graphics graphics = bi.getGraphics();
                graphics.drawImage(converter.getBufferedImage(frame), 0, 0, 568, 320, null);

                //bi_copy.setData(bi.getData());

                //byte[] buffer = FaceTool.decodeToPixels(bi);
                //byte[] buffer_copy = FaceTool.decodeToPixels(bi_copy);

                //Frame newFrame = new Frame("1", lastNumber, Frame.JPG_IMAGE, buffer, buffer_copy,
                        //System.currentTimeMillis(), new Rectangle(0, 0, 1280, 720));

                frameQueue.put(bi);
                System.out.println("Grab queue size is " + frameQueue.size());
            } catch (FrameGrabber.Exception e) {
                continue;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
