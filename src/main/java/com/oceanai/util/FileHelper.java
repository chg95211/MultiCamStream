package com.oceanai.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import sun.misc.BASE64Decoder;

import java.io.*;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by taozhiheng on 16-7-12.
 * helper to handle local file
 */
public class FileHelper {


    /**
     * download image from url
     * */
    public static boolean download(OutputStream os, String urlString)
    {
        if(urlString == null)
            return false;
        try {
            URL url = new URL(urlString);
            URLConnection conn = url.openConnection();
            InputStream is = conn.getInputStream();
            byte[] buf = new byte[1024];
            int size;
            while((size = is.read(buf))>-1)
            {//循环读取
                os.write(buf, 0, size);
            }
            os.flush();
            is.close();
            return true;
        }
        catch (MalformedURLException e)
        {
            e.printStackTrace();
            return false;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static String readString(String filePath)
    {
        String text = "";
        File file = new File(filePath);
        try {
            FileInputStream fis = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
            char[] buffer= new char[512];
            int count;
            StringBuilder builder = new StringBuilder();
            while((count = reader.read(buffer)) != -1)
            {
                builder.append(buffer, 0, count);
            }
            text = builder.toString();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return text;
    }


    public static byte[] base64Decode(String src)
    {
        if(src == null)
            return null;
        byte[] data;
        BASE64Decoder decoder = new BASE64Decoder();
        try {
            data = decoder.decodeBuffer(src);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void readByteFile(String filename, OutputStream os) throws IOException
    {
        readByteFile(new File(filename), os);
    }

    public static void readByteFile(File file, OutputStream os) throws IOException {
        InputStream is = new FileInputStream(file);
        byte[] buf = new byte[1024];
        int size;
        while((size = is.read(buf)) > -1)
        {
            os.write(buf, 0, size);
        }
        os.flush();
        is.close();
    }

    public static void writeByteFile(byte[] data, String filename) throws IOException {
        writeByteFile(data, new File(filename));
    }

    public static void writeByteFile(byte[] data, File file) throws IOException {
        writeByteFile(data, new FileOutputStream(file), true);
    }

    public static void writeByteFile(byte[] data, OutputStream os, boolean close) throws IOException {
        os.write(data);
        if(close)
            os.close();
    }

    public static void changeSDPFile(String ip) {
        String uri = "hdfs://cloud04:9000/user/hadoop/test/test.sdp";
        String content = "m=video 80 RTP/AVP 96\n" +
                "a=rtpmap:96 H264/90000\n" +
                "a=framerate:30\n" +
                "c=IN IP4 ";
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
