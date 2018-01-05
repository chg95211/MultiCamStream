package com.oceanai.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;

public class HDFSHelper implements Serializable {

    public static HDFSHelper hdfsHelper = null;
    private String ip;
    private URI uri;
    private FileSystem fs;

    public static HDFSHelper getInstance() {
        if (hdfsHelper == null)
            hdfsHelper = new HDFSHelper(null);
        return hdfsHelper;
    }

    static {
        if (hdfsHelper == null)
            hdfsHelper = new HDFSHelper(null);
    }

    private HDFSHelper(String ip) {
        this.ip = ip;
    }

    // upload local file to hdfs method 1
    public boolean upload1(String local, String remote) {
        return upload(new File(local), remote);
    }

    // upload local file to hdfs method 2
    public static boolean upload2(String local, String remote) {

        if (StringUtils.isBlank(local) || StringUtils.isBlank(remote)) {
            return false;
        }

        String uri = "hdfs://cloud06:8020/user/hadoop/";
        remote = uri + remote;
        Configuration config = new Configuration();
        FileSystem hdfs;
        try {
            hdfs = FileSystem.get(URI.create(uri), config);
            Path src = new Path(local);
            Path dst = new Path(remote);
            hdfs.copyFromLocalFile(src, dst);
            hdfs.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public boolean upload(File file, String remote) {
        if (!file.exists())
            return false;
        if (file.isFile()) {
            try {
                return upload(new BufferedInputStream(new FileInputStream(file)), remote);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return false;
            }
        } else {
            File[] files = file.listFiles();

            if (files != null) {
                int size = files.length;
                for (File f : files) {
                    if (upload(f, remote + File.separator + f.getName()))
                        size--;
                }
                return size == 0;
            }
            return false;
        }
    }

    public boolean upload(InputStream is, String remote) {

        String dst;
        if (ip != null)
            dst = ip + File.separator + remote;
        else
            dst = remote;
        try {
            open(dst);
            if (fs == null)
                return false;
            OutputStream out = fs.create(new Path(dst));
            IOUtils.copyBytes(is, out, 4096, true);

            return true;
        } catch (IOException e) {
            e.printStackTrace();

            return false;
        }
    }

    public boolean download(String local, String remote) {
        return download(new File(local), remote);
    }

    public boolean download(File file, String remote) {
        if (!file.exists() || file.isFile()) {
            try {
                OutputStream os = new FileOutputStream(file);
                boolean res = download(os, remote);
                os.close();
                return res;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    public boolean download(OutputStream os, String remote) {
        String dst;
        if (ip != null)
            dst = ip + File.separator + remote;
        else
            dst = remote;
        try {
            if (fs == null)
                return false;
            Configuration configuration = new Configuration();
            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            fs.setConf(configuration);
            System.out.println("Configuration finished");
            open(dst);
            Path path = new Path(dst);
            if (!fs.exists(path)) {
                return false;
            }
            FSDataInputStream fsDataInputStream = fs.open(path);
            IOUtils.copyBytes(fsDataInputStream, os, 1024, false);
            fsDataInputStream.close();

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean delete(String remote) {
        String dst;
        if (ip != null)
            dst = ip + File.separator + remote;
        else
            dst = remote;
        try {
            open(dst);
            if (fs == null)
                return false;
            fs.deleteOnExit(new Path(dst));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void open(String url) {
        URI curUri = URI.create(url);
        if (fs != null && uri != null) {
            if (uri.getScheme() == null) {
                if (curUri.getScheme() == null)
                    return;
            } else if (uri.getScheme().equals(curUri.getScheme())) {
                return;
            }
            if (uri.getAuthority() == null) {
                if (curUri.getAuthority() == null)
                    return;
            } else if (uri.getAuthority().equals(curUri.getAuthority())) {
                return;
            }
        }
        uri = curUri;
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            fs = FileSystem.get(uri, conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (fs != null) {
            try {
                fs.close();
                fs = null;
                uri = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
