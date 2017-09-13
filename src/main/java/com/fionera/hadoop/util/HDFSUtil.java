package com.fionera.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

@SuppressWarnings("unused")
public class HDFSUtil {
    private static final String HDFS_URL = "hdfs://single:9000";
    private static FileSystem sFileSystem;

    private static FileSystem getFileSystem() throws URISyntaxException, IOException {
        if (sFileSystem != null) {
            return sFileSystem;
        }
        Configuration conf = new Configuration();
        conf.set("fs.dfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        URI uri = new URI(HDFS_URL);
        return sFileSystem = FileSystem.get(uri, conf);
    }

    // 读取文件
    public static void readFile(String hdfsFile) throws Exception {
        FileSystem fileSystem = getFileSystem();

        FSDataInputStream openStream = null;
        try {
            openStream = fileSystem.open(new Path(HDFS_URL + hdfsFile));
            IOUtils.copyBytes(openStream, System.out, 1024, false);
        } finally {
            if (openStream != null) {
                IOUtils.closeStream(openStream);
            }
        }
    }

    // 写入文件
    public static void writeFile(String sourceFile, String hdfsFile) throws Exception {
        FileSystem fs = getFileSystem();

        String destination = HDFS_URL + hdfsFile;
        InputStream in = new BufferedInputStream(new FileInputStream(sourceFile));
        FSDataOutputStream out = fs.create(new Path(destination));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    // 创建文件夹
    public static void mkDir(String hdfsFile) throws Exception {
        FileSystem fileSystem = getFileSystem();

        fileSystem.mkdirs(new Path(HDFS_URL + hdfsFile));
    }

    // 删除文件夹
    public static void rmDir(String hdfsFile) throws Exception {
        FileSystem fileSystem = getFileSystem();

        boolean isDeleted = fileSystem.delete(new Path(HDFS_URL + hdfsFile), false);
        System.out.println(isDeleted ? "delete succeed." : "delete failed.");
    }

    // 查看文件是否存在
    public static boolean checkFile(String hdfsFile) throws Exception {
        FileSystem fileSystem = getFileSystem();
        boolean isExist = fileSystem.exists(new Path(HDFS_URL + hdfsFile));
        System.out.println(isExist ? "file exist." : "file non exist.");
        return isExist;
    }

    // 遍历FileSystem
    public static void list(String hdfsFile) throws Exception {
        FileSystem fileSystem = getFileSystem();
        FileStatus[] listStatus = fileSystem.listStatus(new Path(HDFS_URL + hdfsFile));
        for (FileStatus fileStatus : listStatus) {
            String isDir = fileStatus.isDirectory() ? "DIR" : "file";
            String name = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + name);
        }
    }

    // 文件存储的位置信息
    public static void locationFile(String hdfsFile) throws Exception {
        FileSystem fileSystem = getFileSystem();
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(HDFS_URL + hdfsFile));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0,
                fileStatus.getLen());
        int blockLen = blockLocations.length;
        for (int i = 0; i < blockLen; i++) {
            String[] hosts = blockLocations[i].getHosts();
            System.out.println("block_" + i + "_location:" + hosts[0]);
        }
    }

    public static void main(String[] args) throws Exception {
        String hadoopFile = "/user/hadoop/data/input/test.txt";
        readFile(hadoopFile);
    }
}
