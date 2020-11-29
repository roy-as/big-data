package com.roy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;

public class HDFSTest {

    @Test
    public void test1() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.22.32:8020");
        FileSystem fs = FileSystem.newInstance(conf);

//        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("/test"), true);
//        while (it.hasNext()) {
//            LocatedFileStatus file = it.next();
//            System.out.println(file.getPath().getName());
//        }
        //fs.create(new Path("/export/servers"));
        fs.copyToLocalFile(new Path("/hello.txt"), new Path("/Users/aby/Desktop/"));
        //fs.copyFromLocalFile(new Path("/Users/aby/Desktop/照片/1.jpeg"), new Path("/export/data/"));
        fs.close();
    }

}
