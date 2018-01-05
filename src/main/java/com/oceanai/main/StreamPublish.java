package com.oceanai.main;

import com.google.gson.Gson;
import com.oceanai.api.MIH;
import com.oceanai.kafka.KafkaConsumer;
import com.oceanai.model.Configuration;
import com.oceanai.util.FileHelper;
import com.oceanai.util.HttpClientUtil;

import java.io.File;

/**
 * Created by WangRupeng on 2017/11/1.
 */
public class StreamPublish {
    public static void main(String[] args) {
        String zookeeper = "cloud04:2181/kafka";
        String groupId = "group1";
        String topic = "topic-stream-operation";
        //String topic = "topic-file-storage";
        int threads = 1;
        Gson gson = new Gson();
        Configuration configuration = new Configuration();
        if (args.length > 0) {
            String text = FileHelper.readString(args[0]);
            configuration = gson.fromJson(text, Configuration.class);
            HttpClientUtil.setURI(configuration.server_ip);
            MIH.setThreshold(configuration.threshold);
            System.out.println("Http face api server is " + HttpClientUtil.getUri());
            System.out.println("Threshold is " + MIH.getThreshold());
            //zookeeper = configuration.server_ip + zookeeper;
        }

        KafkaConsumer example = new KafkaConsumer(topic, threads, zookeeper, groupId, configuration.bitrate, configuration.rtp_server_ip, configuration.rtp_port);
        new Thread(example).start();
    }
}
