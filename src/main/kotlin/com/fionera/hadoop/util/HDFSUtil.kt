package com.fionera.hadoop.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.net.URI

@Suppress("unused")
object HDFSUtil {
    private val HDFS_URL = "hdfs://single:9000"
    private var sFileSystem: FileSystem? = null

    private val fileSystem: FileSystem
        @Throws(Exception::class)
        get() {
            if (sFileSystem == null) {
                val conf = Configuration()
                conf.set("fs.dfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem::class.java.name)
                val uri = URI(HDFS_URL)
                sFileSystem = FileSystem.get(uri, conf)
            }
            return sFileSystem!!
        }

    // 读取文件
    @Throws(Exception::class)
    fun readFile(hdfsFile: String) {
        val fileSystem = fileSystem

        var openStream: FSDataInputStream? = null
        try {
            openStream = fileSystem.open(Path(HDFS_URL + hdfsFile))
            IOUtils.copyBytes(openStream!!, System.out, 1024, false)
        } finally {
            if (openStream != null) {
                IOUtils.closeStream(openStream)
            }
        }
    }

    // 写入文件
    @Throws(Exception::class)
    fun writeFile(sourceFile: String, hdfsFile: String) {
        val fs = fileSystem

        val destination = HDFS_URL + hdfsFile
        val `in` = BufferedInputStream(FileInputStream(sourceFile))
        val out = fs.create(Path(destination))
        IOUtils.copyBytes(`in`, out, 4096, true)
    }

    // 创建文件夹
    @Throws(Exception::class)
    fun mkDir(hdfsFile: String) {
        val fileSystem = fileSystem

        fileSystem.mkdirs(Path(HDFS_URL + hdfsFile))
    }

    // 删除文件夹
    @Throws(Exception::class)
    fun rmDir(hdfsFile: String) {
        val fileSystem = fileSystem

        val isDeleted = fileSystem.delete(Path(HDFS_URL + hdfsFile), false)
        println(if (isDeleted) "delete succeed." else "delete failed.")
    }

    // 查看文件是否存在
    @Throws(Exception::class)
    fun checkFile(hdfsFile: String): Boolean {
        val fileSystem = fileSystem
        val isExist = fileSystem.exists(Path(HDFS_URL + hdfsFile))
        println(if (isExist) "file exist." else "file non exist.")
        return isExist
    }

    // 遍历FileSystem
    @Throws(Exception::class)
    fun list(hdfsFile: String) {
        val fileSystem = fileSystem
        val listStatus = fileSystem.listStatus(Path(HDFS_URL + hdfsFile))
        for (fileStatus in listStatus) {
            val isDir = if (fileStatus.isDirectory) "DIR" else "file"
            val name = fileStatus.path.toString()
            println(isDir + "\t" + name)
        }
    }

    // 文件存储的位置信息
    @Throws(Exception::class)
    fun locationFile(hdfsFile: String) {
        val fileSystem = fileSystem
        val fileStatus = fileSystem.getFileStatus(Path(HDFS_URL + hdfsFile))
        val blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0,
                fileStatus.len)
        val blockLen = blockLocations.size
        for (i in 0 until blockLen) {
            val hosts = blockLocations[i].hosts
            println("block_" + i + "_location:" + hosts[0])
        }
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val hadoopFile = "/user/hadoop/data/input/test.txt"
        readFile(hadoopFile)
    }
}
