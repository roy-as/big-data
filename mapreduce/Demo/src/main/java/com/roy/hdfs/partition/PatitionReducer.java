package com.roy.hdfs.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PatitionReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

    public PatitionReducer() {
        System.out.println(this);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value, NullWritable.get());
        }
    }
}
