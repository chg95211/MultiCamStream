package com.oceanai.util;

import org.apache.http.*;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import sun.misc.BASE64Encoder;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;

/**
 * HttpClient工具类
 * Created by Wangke on 2017/11/14.
 */
public class HttpClientUtil {

    private static CloseableHttpClient httpClient = null;
    private final static Object syncLock = new Object();
    private static String uri = "http://cloud04:33388/detect";

    public static void setURI(String ip) {
        uri = "http://" + ip + ":33388/detect";
    }

    public static String getUri() {
        return uri;
    }

    /**
     * 获取HttpClient对象
     */
    public static CloseableHttpClient getHttpClient(String url) {
        String hostname = url.split("/")[2];
        int port = 80;
        if (hostname.contains(":")) {
            String[] arr = hostname.split(":");
            hostname = arr[0];
            port = Integer.parseInt(arr[1]);
        }
        if (httpClient == null) {
            synchronized (syncLock) {
                if (httpClient == null) {
                    httpClient = createHttpClient(200, 40, 100, hostname, port);
                }
            }
        }
        return httpClient;
    }

    /**
     * 创建HttpClient对象
     */
    public static CloseableHttpClient createHttpClient(int maxTotal, int maxPerRoute, int maxRoute, String hostname, int port) {

        ConnectionSocketFactory plainsf = PlainConnectionSocketFactory.getSocketFactory();
        LayeredConnectionSocketFactory sslsf = SSLConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> registry = RegistryBuilder
                .<ConnectionSocketFactory>create().register("http", plainsf)
                .register("https", sslsf).build();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(
                registry);

        cm.setMaxTotal(maxTotal);// 将最大连接数增加
        cm.setDefaultMaxPerRoute(maxPerRoute);// 将每个路由基础的连接增加
        cm.setMaxPerRoute(new HttpRoute(new HttpHost(hostname, port)), maxRoute); // 将目标主机的最大连接数增加

        // 请求重试处理
        HttpRequestRetryHandler httpRequestRetryHandler = (exception, executionCount, context) -> {

            if (executionCount >= 5) {// 如果已经重试了5次，就放弃
                return false;
            }
            if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
                return true;
            }
            if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
                return false;
            }
            if (exception instanceof InterruptedIOException) {// 超时
                return false;
            }
            if (exception instanceof UnknownHostException) {// 目标服务器不可达
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {// 连接被拒绝
                return false;
            }
            if (exception instanceof SSLException) {// SSL握手异常
                return false;
            }

            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            if (!(request instanceof HttpEntityEnclosingRequest)) {// 如果请求是幂等的，就再次尝试
                return true;
            }

            return false;
        };

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setRetryHandler(httpRequestRetryHandler).build();

        return httpClient;
    }

    public static String post(String base64) {

        long time = System.currentTimeMillis();
        JSONObject jsonParam = new JSONObject();
        try {
            jsonParam.put("api_key", "");
            jsonParam.put("image_base64", base64);
            jsonParam.put("field", "feature");
        } catch (JSONException e) {
            e.printStackTrace();
        }

        StringEntity entity = new StringEntity(jsonParam.toString(), "UTF-8");
        entity.setContentEncoding("UTF-8");
        entity.setContentType("application/json");

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(10000)
                .setConnectTimeout(10000).setSocketTimeout(10000).build();
        HttpPost httppost = new HttpPost(uri);
        httppost.setConfig(requestConfig);
        httppost.setHeader("Connection", "close");
        httppost.setEntity(entity);

//        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpClient httpClient = getHttpClient(uri);

        try {
            CloseableHttpResponse response = httpClient.execute(httppost);
            try {
                HttpEntity entity2 = response.getEntity();
                return EntityUtils.toString(entity2, "UTF-8");
            } finally {
                response.close();
                System.out.println(System.currentTimeMillis() - time);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return null;
    }

    public static void main(String[] args) {

        int pageCount = 100;
        String base64 = readFromFile("E:\\HUST\\HUST-BitData\\FinderQJ\\resources\\images\\realtime\\wangke01.jpg");
       /* ExecutorService executors = Executors.newFixedThreadPool(2); //100? 每个线程里会创建一个HttpClient对象，无论是连接池还是createDefault()

        for (int i = 0; i < pageCount; i++) {
            executors.execute(() -> System.out.println(post(base64)));
        }

        executors.shutdown();*/

        System.out.println(post(base64));
    }

    public static String readFromFile(String file) {
        InputStream in = null;
        byte[] data = null;

        try {
            in = new FileInputStream(file);
            data = new byte[in.available()];
            in.read(data);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        BASE64Encoder encoder = new BASE64Encoder();
        return encoder.encode(data);
    }
}