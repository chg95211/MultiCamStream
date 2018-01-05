package com.oceanai.test;

import com.google.gson.Gson;
import com.oceanai.util.HBaseHelper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * Created by WangRupeng on 2017/11/23.
 */
public class FinderRealtimeHbaseTest {
    private HBaseHelper hbaseHelper ;
    private String table = "realtime_faces_table";
    private String family = "info";
    private String[] columns = {"camera_id", "face_url", "feature", "hashkey", "origin_url", "person_uid", "timestamp"};
    private Gson gson = new Gson();

    public FinderRealtimeHbaseTest() {
        hbaseHelper = new HBaseHelper("192.168.1.5", 2181);
    }

    public void scan() {
        if (hbaseHelper != null) {
            try {
                ResultScanner results = hbaseHelper.getAllRows(table);

                int count = 0;
                for (Result result : results) {
                    String rowKey = result.getRow().toString();
                    float[] feature = gson.fromJson(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(columns[2]))), float[].class);
                    String url = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(columns[1])));
                    String person_uid = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(columns[5])));
                    String camera_id = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(columns[0])));

                    count++;
                    if (count < 10) {
                        System.out.println("====================================");
                        System.out.println("person_uid is " + person_uid);
                        System.out.println("feature size is " + feature.length);
                        if (feature.length < 10){
                            System.out.println(Arrays.toString(feature));
                        }
                        System.out.println("url is " + url);
                        System.out.println("camera_id is " + camera_id);
                        System.out.println("====================================");
                    }
                }

                System.out.println("Row number is " + count);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
    public static void main(String[] args) {
        FinderRealtimeHbaseTest test = new FinderRealtimeHbaseTest();
        long start = System.currentTimeMillis();
        test.scan();
        System.out.println("Scan time used " + (System.currentTimeMillis() - start) + "ms");
    }
}
