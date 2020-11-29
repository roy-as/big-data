package com.roy.hdfs.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WorldCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k2 = new Text();
    private IntWritable v2 = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String k1 = value.toString();
        if(StringUtils.isNotBlank(k1)) {
            String[] words = k1.split(" ");
            for (String word : words) {
                k2.set(word);
                context.write(k2, v2);
            }
        }
    }
}
