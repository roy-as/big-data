package com.roy.hdfs.partition;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PatitionMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable k2 = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String v1 = value.toString();
        if(StringUtils.isNotBlank(v1)) {
            String[] datas = v1.split("\t");
            k2.set(Integer.valueOf(datas[5]));
            context.write(k2, value);
        }
    }
}
