package com.roy.hdfs.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ExamplePatition extends Partitioner<IntWritable, Text> {


    @Override
    public int getPartition(IntWritable k2, Text v2, int taskNumber) {
        return k2.get() < 14 ? 0 : 1;
    }
}
