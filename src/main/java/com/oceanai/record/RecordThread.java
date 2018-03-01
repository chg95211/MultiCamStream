package com.oceanai.record;

import com.oceanai.util.FileLogger;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;
import javax.imageio.ImageIO;
import org.bytedeco.javacpp.avcodec;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameRecorder;
import org.bytedeco.javacv.Java2DFrameConverter;

/**
 *推流线程.
 *Created by WangRupeng on 2017/12/2.
 *@author <a href="http://datacoder.top">王汝鹏</a>
 *@version
 */
public class RecordThread implements Runnable {

  private static final String TAG = "RecordThread";
  private FileLogger fileLogger;
  private boolean running = false;
  private LinkedBlockingQueue<BufferedImage> frameQueue2;

  private FFmpegFrameRecorder recorder;
  private Java2DFrameConverter converter;
  private String destLocation = "";
  private int bitrate = 40;
  private boolean isRestarting = false;
  private BufferedImage testBufferedImage = null;

  /**
   * 从recordQueue队列中读取帧数据，并推流到指定地址.
   * @param recordQueue 推流消费队列
   * @param bitrate 推流比特率
   * @param logDir 日志地址
   */
  public RecordThread(LinkedBlockingQueue<BufferedImage> recordQueue, int bitrate, String logDir) {
    this.frameQueue2 = recordQueue;
    converter = new Java2DFrameConverter();
    this.bitrate = bitrate;
    if (!logDir.endsWith("/")) {
      logDir = logDir + "/";
    }
    fileLogger = new FileLogger(logDir + "RecordThread.log");
    try {
      testBufferedImage = ImageIO
          .read(new File("/home/hadoop/wrp/RealtimeStreamPublish/java/images/loading.jpg"));
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  /**
   *
   * @param location 推流地址
   * @throws Exception
   */
  public void start(String location) throws Exception {
    fileLogger.log(TAG, "Recorder ready to start! Bitrate is " + bitrate);
    if (recorder != null) {
      //recorder.stop();
      //recorder.release();
      recorder = null;
    }
    destLocation = location;
    recorder = new FFmpegFrameRecorder(destLocation/*"rtp://11.185.201.100:8012"*/, 1280, 720);
    recorder.setFormat("rtp");
    recorder.setFrameRate(25);
    recorder.setVideoBitrate(bitrate * 10000);
    recorder.setVideoQuality(0);
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

  public void grabberRestarting(boolean isRestarting) {
    this.isRestarting = isRestarting;
  }

  @Override
  public void run() {
    try {
      long startTime = System.currentTimeMillis();
      while (running) {
        long start = System.currentTimeMillis();
        BufferedImage bufferedImage = frameQueue2.poll();
        if (frameQueue2.size() > 90) {
          continue;
        }
        if (bufferedImage != null && recorder != null) {
          Frame ff = converter.convert(bufferedImage);
          long videoTS = 1000 * (System.currentTimeMillis() - startTime);
          recorder.setTimestamp(videoTS);
          recorder.record(ff);
          System.out.println(
              "Record one frame, record queue size is " + frameQueue2.size() + " time used " + (
                  System.currentTimeMillis() - start));
          fileLogger.log(TAG,
              "Record one frame, record queue size is " + frameQueue2.size() + " time used " + (
                  System.currentTimeMillis() - start));
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
      stop();
    }
    String rtpAddress = "rtp://" + location + ":" + port;
    System.out.println("RTP address is " + rtpAddress);
    fileLogger.log(TAG, "Set RTP address " + rtpAddress);
    try {
      start(rtpAddress);
    } catch (Exception e) {
      //e.printStackTrace();
    }
    return true;
  }

  public boolean isRunning() {
    return running;
  }
}
