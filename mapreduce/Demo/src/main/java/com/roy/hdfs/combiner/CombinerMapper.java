package com.roy.hdfs.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CombinerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable v2 = new IntWritable(1);

    private Text k2 = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String k1 = value.toString();
        if(k1.contains("入门宝典")) {
            k2.set("IT技术");
        } else if(k1.contains("葵花宝典") || k1.contains("论清王朝的腐败")) {
            k2.set("历史");
        } else {
            k2.set("武功秘籍");
        }
        context.write(k2, v2);
    }
}
