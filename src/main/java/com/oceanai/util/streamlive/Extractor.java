package com.oceanai.util.streamlive;

import com.oceanai.util.FileLogger;

import java.awt.image.BufferedImage;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Wangke on 2017/9/25.
 */
public class Extractor extends Thread {
    private static final String TAG = "Extractor";
    private FileLogger fileLogger;
    private LinkedBlockingQueue<BufferedImage> frameQueue;
    private LinkedBlockingQueue<BufferedImage> frameQueue2;
    private String logDir = "";

    public Extractor(LinkedBlockingQueue<BufferedImage> frameQueue, LinkedBlockingQueue<BufferedImage> frameQueue2, String logDir) {
        this.frameQueue = frameQueue;
        this.frameQueue2 = frameQueue2;
        if (!logDir.endsWith("/")) {
            logDir = logDir + "/";
        }
        this.logDir = logDir;
        fileLogger = new FileLogger(logDir + "Extractor.log");
    }

    public void run() {
        Stream operation = null;
        try {
            operation = new Stream(logDir);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        fileLogger.log(TAG, "Extractor started!");
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
                System.out.println(e1.getMessage());
            }
        }
    }
}