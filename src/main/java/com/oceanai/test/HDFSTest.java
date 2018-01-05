package com.oceanai.test;

import com.oceanai.util.HDFSHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by WangRupeng on 2017/12/11.
 */
public class HDFSTest {
    public static void main(String[] args) {
       /* String hdfs = "hdfs://cloud04:8020/user/hadoop/test/test.sdp";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        HDFSHelper.getInstance().download(outputStream, hdfs);
        //HDFSHelper.getInstance().download("/home/hadoop/wrp/RealtimeStreamPublish/java/", hdfs);
        String content = outputStream.toString();
        System.out.println(content);*/

        String uri = "hdfs://cloud04:9000/user/hadoop/test/test.sdp";
        String content = "m=video 80 RTP/AVP 96\n" +
                "a=rtpmap:96 H264/90000\n" +
                "a=framerate:30\n" +
                "c=IN IP4 ";
        String ip = "192.168.1.103";
        InputStream in = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create (uri), conf);
            Path path = new Path(uri);
            //in = fs.open( new Path(uri));
            //IOUtils.copyBytes(in, System.out, 4096, false);
            if (fs.delete(path, true)) {
                System.out.println("File deleted, now create new file");
                FSDataOutputStream outputStream = fs.create(path);
                outputStream.write((content + ip).getBytes());
                outputStream.close();
                fs.close();
            }
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                IOUtils.closeStream(in);
            }
        }
    }
}
