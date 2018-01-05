package com.oceanai.util.streamlive;

import com.oceanai.util.FileLogger;

import java.awt.image.BufferedImage;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Wangke on 2017/9/25.
 */
public class Extractor extends Thread {
    private static final String TAG = "Extractor";
    private FileLogger fileLogger = new FileLogger("/home/hadoop/realtime_recorder_logs/Extractor.log");
    private LinkedBlockingQueue<BufferedImage> frameQueue;
    private LinkedBlockingQueue<BufferedImage> frameQueue2;
    private boolean running = false;
    private float threas = 0.75f;

    public Extractor(LinkedBlockingQueue<BufferedImage> frameQueue, LinkedBlockingQueue<BufferedImage> frameQueue2) {
        this.frameQueue = frameQueue;
        this.frameQueue2 = frameQueue2;
    }

    public void run() {
        Stream operation = null;
        try {
            operation = new Stream();
        } catch (Exception e) {
            //e.printStackTrace();
        }
        fileLogger.log(TAG, "Extractor started!");
        //running = true;
        while (true) {
            long now = System.currentTimeMillis();
            try {
                BufferedImage buffer = frameQueue.poll();
                if (buffer != null) {
                    BufferedImage bufferedImage = operation.execute(buffer);
                    if (bufferedImage != null) {
                        if (frameQueue2.size() == 100) {
                            frameQueue2.clear();
                            continue;
                        }
                        frameQueue2.put(bufferedImage);
                        //System.out.println("Detect queue size is " + frameQueue2.size());
                        System.out.println("====== Grab queue 1 " + frameQueue.size());
                        fileLogger.log(TAG, "====== Grab queue 1 " + frameQueue.size());
                        System.out.println("====== Extract queue 2 " + frameQueue2.size());
                        fileLogger.log(TAG, "====== Extract queue 2 " + frameQueue2.size());
                        System.out.println("====== calc consumes: " + (System.currentTimeMillis() - now));
                        fileLogger.log(TAG, "====== calc consumes: " + (System.currentTimeMillis() - now));

                    }
                }
            } catch (Exception e1) {
                //e1.printStackTrace();
            }
        }

        //running = false;
    }
}