package com.roy.hdfs.index;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexCombiner extends Reducer<Word, NullWritable, Word, NullWritable> {

//    private Text k2 = new Text();
//    private IntWritable v2 = new IntWritable();

    @Override
    protected void reduce(Word key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        int sum = 0;
        for (NullWritable value : values) {
            sum += key.getCount();
        }
        key.setCount(sum);
        //key.setCombine(true);
        context.write(key, NullWritable.get());
    }
}


