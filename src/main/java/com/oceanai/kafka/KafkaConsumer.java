package com.oceanai.kafka;

import com.oceanai.grab.GrabFaceTrackerThread;
import com.oceanai.record.RecordThread;
import com.oceanai.util.FileHelper;
import com.oceanai.util.FileLogger;
import com.oceanai.util.streamlive.BaseRecorder;
import com.oceanai.util.streamlive.Extractor;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.awt.image.BufferedImage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by WangRupeng on 2017/10/7.
 */
public class KafkaConsumer implements Runnable {
    /**
     * Kafka数据消费对象
     */
    private ConsumerConnector consumer;

    /**
     * Kafka Topic名称
     */
    private String topic;

    /**
     * 线程数量，一般就是Topic的分区数量
     */
    private int numThreads;

    /**
     * rtp address
     */
    private String rtp_server_ip = "localhost";

    /**
     * rtp port
     */
    private String rtp_port = "8011";

    /**
     * log directory
     */
    private String logDir = "";

    /**
     * rtp stream bitrate
     */
    private int bitrate;

    /**
     * 线程池
     */
    private ExecutorService executorPool;

    private ConsumerKafkaStreamProcesser consumerKafkaStreamProcesser;

    /**
     *日志
     */
    private static final String TAG = "KafkaConsumer";
    private static FileLogger mLogger= new FileLogger("/home/hadoop/realtime_recorder_logs/"+TAG + ".log");

    /**
     * 构造函数
     *
     * @param topic      Kafka消息Topic主题
     * @param numThreads 处理数据的线程数/可以理解为Topic的分区数
     * @param zookeeper  Kafka的Zookeeper连接字符串
     * @param groupId    该消费者所属group ID的值
     */
    public KafkaConsumer(String topic, int numThreads, String zookeeper, String groupId, int bitrate, String server, String rtp_port, String logDir) {
        // 1. 创建Kafka连接器
        this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        // 2. 数据赋值
        this.topic = topic;
        this.numThreads = numThreads;
        this.bitrate = bitrate;
        this.rtp_server_ip = server;
        this.rtp_port = rtp_port;
        this.logDir = logDir;
    }

    @Override
    public void run() {
        // 1. 指定Topic
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(this.topic, this.numThreads);

        // 2. 指定数据的解码器
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        // 3. 获取连接数据的迭代器对象集合
        /**
         * Key: Topic主题
         * Value: 对应Topic的数据流读取器，大小是topicCountMap中指定的topic大小
         */
        Map<String, List<KafkaStream<String, String>>> consumerMap = this.consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);

        // 4. 从返回结果中获取对应topic的数据流处理器
        List<KafkaStream<String, String>> streams = consumerMap.get(this.topic);

        // 5. 创建线程池
        this.executorPool = Executors.newFixedThreadPool(this.numThreads);



        // 6. 构建数据输出对象
        int threadNumber = 0;
        for (final KafkaStream<String, String> stream : streams) {
            this.consumerKafkaStreamProcesser = new ConsumerKafkaStreamProcesser(stream, threadNumber);
            this.executorPool.submit(this.consumerKafkaStreamProcesser);
            threadNumber++;
        }
    }

    public void shutdown() {
        // 1. 关闭和Kafka的连接，这样会导致stream.hashNext返回false
        if (this.consumer != null) {
            this.consumer.shutdown();
        }

        // 2. 关闭线程池，会等待线程的执行完成
        if (this.executorPool != null) {
            // 2.1 关闭线程池
            this.executorPool.shutdown();

            // 2.2. 等待关闭完成, 等待五秒
            try {
                if (!this.executorPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly!!");
                }
            } catch (InterruptedException e) {
                System.out.println("Interrupted during shutdown, exiting uncleanly!!");
            }
        }

    }

    /**
     * 根据传入的zk的连接信息和groupID的值创建对应的ConsumerConfig对象
     *
     * @param zookeeper zk的连接信息，类似于：<br/>
     *                  hadoop-senior01.ibeifeng.com:2181,hadoop-senior02.ibeifeng.com:2181/kafka
     * @param groupId   该kafka consumer所属的group id的值， group id值一样的kafka consumer会进行负载均衡
     * @return Kafka连接信息
     */
    private ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        // 1. 构建属性对象
        Properties prop = new Properties();
        // 2. 添加相关属性
        prop.put("group.id", groupId); // 指定分组id
        prop.put("zookeeper.connect", zookeeper); // 指定zk的连接url
        prop.put("zookeeper.session.timeout.ms", "10000"); //
        prop.put("zookeeper.sync.time.ms", "5000");
        prop.put("auto.commit.interval.ms", "5000");
        // 3. 构建ConsumerConfig对象
        return new ConsumerConfig(prop);
    }

    /**
     * Kafka消费者数据处理线程
     */
    public class ConsumerKafkaStreamProcesser implements Runnable {
        private String TAG = "ConsumerKafkaStreamProcesser";
        // Kafka数据流
        private KafkaStream<String, String> stream;
        // 线程ID编号
        private int threadNumber;

        /**
         * 本地推流的地址
         */
        private String streamLocation = "";

        /**
         * kafka 消息（ip*streamLocation）
         */
        private String kafkaMessage = "";

        private GrabFaceTrackerThread grabThread;
        private RecordThread recordThread;
        private Extractor ex;
        private Thread grab_t;
        private Thread record_t;

        private LinkedBlockingQueue<BufferedImage> frames = new LinkedBlockingQueue<>(100);
        private LinkedBlockingQueue<BufferedImage> detectedFrames = new LinkedBlockingQueue<>(100);

        public ConsumerKafkaStreamProcesser(KafkaStream<String, String> stream, int threadNumber) {
            this.stream = stream;
            this.threadNumber = threadNumber;

            grabThread = new GrabFaceTrackerThread(frames, logDir);
            ex = new Extractor(frames, detectedFrames, logDir);
            ex.start();
            //br.start();
            recordThread = new RecordThread(detectedFrames, bitrate, logDir);
            //System.out.println("ServerIP is " + rtp_server_ip + " RTP port is " + rtp_port);
            recordThread.setLocation(rtp_server_ip, rtp_port);
            if (record_t != null) {
                record_t = null;
            }
            new Thread(recordThread).start();
        }

        @Override
        public void run() {
            // 1. 获取数据迭代器
            ConsumerIterator<String, String> iter = this.stream.iterator();

            // 2. 迭代输出数据
            while (iter.hasNext()) {
                // 2.1 获取数据值
                MessageAndMetadata value = iter.next();

                // 2.2 输出
                System.out.println(this.threadNumber + ":" + ":" + value.offset() + value.key() + ":" + value.message());
                mLogger.log(TAG + "@" + this.threadNumber, this.threadNumber + ":" + ":" + value.offset() + value.key() + ":" + value.message());

                kafkaMessage = (String) value.message();

                String[] msg = kafkaMessage.split("\\*");

                if (msg.length != 2 || !msg[1].startsWith("rtsp")) {
                    continue;
                }

                streamLocation = msg[1];

                recordThread.grabberRestarting(true);

                grabThread.setLocation(streamLocation);
                if (grab_t != null) {
                    grab_t = null;
                }
                grab_t = new Thread(grabThread);
                grab_t.start();
                recordThread.grabberRestarting(false);
                if (!recordThread.isRunning()) {
                    recordThread.setLocation(rtp_server_ip, rtp_port);
                    if (record_t != null) {
                        record_t = null;
                    }
                    record_t = new Thread(recordThread);
                    record_t.start();
                }


                try {
                    Thread.sleep(2000);
                } catch (Exception e) {
                    System.out.println("Grabber and recorder is starting");
                }
            }

            // 3. 表示当前线程执行完成
            System.out.println("Shutdown Thread:" + this.threadNumber);
            mLogger.log(TAG+ "@" + this.threadNumber, "Shutdown Thread:" + this.threadNumber);
        }

        //get camera the last block of local ip as the stream id
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

    }
}
