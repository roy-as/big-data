package com.roy.hdfs.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SortWriteable, Text, Text, NullWritable> {

    private NullWritable v3 = NullWritable.get();

    @Override
    protected void reduce(SortWriteable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, v3);
        }
    }
}
