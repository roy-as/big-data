package com.roy.hdfs.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text, Order, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String k1 = value.toString();
        String[] strs = k1.split("\t");
        Order order = new Order(strs[0], strs[1], Double.valueOf(strs[2]));
        context.write(order, value);
    }
}
