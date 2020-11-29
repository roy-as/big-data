package com.roy.hdfs.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class IndexMapper extends Mapper<LongWritable, Text,Word , NullWritable> {

    private NullWritable v2 = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String k1 = value.toString();
        String name = fileSplit.getPath().getName();
        String[] words = k1.split(" ");

        for (String str : words) {
            Word word = new Word(name, str, 1, false);
            context.write(word, v2);
        }

    }
}
