package com.roy.hdfs.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<Order, Text, Text, NullWritable> {

    private NullWritable v3 = NullWritable.get();

    @Override
    protected void reduce(Order key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text k3 : values) {
            context.write(k3, v3);
        }
    }
}
