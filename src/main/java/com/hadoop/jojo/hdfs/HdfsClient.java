package com.hadoop.jojo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * description:
 * hdfs API 操作实例
 *
 * @author sunjiamin
 * @date 2018-05-09 09:33
 */
public class HdfsClient {
    /**
     * logback
     */
    private final static Logger logger = LoggerFactory.getLogger(HdfsClient.class);

    public static  String HDFS_URL="hdfs://node-100";
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        conf = new Configuration();
        conf.set("fs.defaultFS", HdfsClient.HDFS_URL);

        //拿到一个文件系统操作的客户端实例对象
        fs = FileSystem.get(new URI(HdfsClient.HDFS_URL),conf,"jojo");
    }

    /**
     * 测试读取目录
     */
    @Test
    public void readDir() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath() + "======" + fileStatus.toString());
        }

        // //会递归找到所有的文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName() + "-------->" + fileStatus.getPath().toString());
        }
    }

    /**
     * 测试创建目录
     */
    @Test
    public void mkDir() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/jojo"));
        System.out.println("创建目录："+mkdirs);
    }
    /**
     * 删除目录
     */
    @Test
    public void deleteDir() throws Exception{
        //true， 递归删除
        boolean delete = fs.delete(new Path("/jojo"), true);
        System.out.println("删除:"+delete);
    }

    /**
     * 测试本地文件上传到hdfs
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        //true， 递归删除
        fs.copyFromLocalFile(new Path("/Users/sunjiamin/资料/Hadoop3.0分布式集群搭建(HA)详细文档.docx"),new Path("/sunjiamin"));
        fs.close();
    }
    /**
     * 测试本地文件上传到hdfs
     * @throws Exception
     */
    @Test
    public void deleteFile() throws Exception{
        //true， 递归删除
        boolean delete = fs.delete(new Path("/sunjiamin/Hadoop3.0分布式集群搭建(HA)详细文档.docx"), true);
        System.out.println("删除:"+delete);
    }

    /**
     * 测试读取文件内容
     * @throws Exception
     */
    @Test
    public void readFile() throws Exception{
        //true， 递归删除
        FSDataInputStream fsDataInputStream = fs.open(new Path("/sunjiamin/testhdfs.txt"));
        //第一种读法
        //IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
        //IOUtils.closeStream(fsDataInputStream);

        //第二种读法
//        byte[] buf = new byte[1024];
//        int bytesRead = fsDataInputStream.read(buf);
//        while (bytesRead!=-1){
//            System.out.write(buf, 0, bytesRead);
//            bytesRead = fsDataInputStream.read(buf);
//        }


        InputStreamReader reader = new InputStreamReader(fsDataInputStream);
        //第三种读法
//        String results = "";
//        int tmp  ;
//        while ((tmp = reader.read()) !=-1){
//            results += (char)tmp;
//        }
//        System.out.println(results);



        //第四种读法
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = "";
        String lineResult = "";
        while ((line = bufferedReader.readLine() )!= null){
            lineResult +=line+"\n";
        }

        System.out.println(lineResult);

        fsDataInputStream.close();
    }

}
