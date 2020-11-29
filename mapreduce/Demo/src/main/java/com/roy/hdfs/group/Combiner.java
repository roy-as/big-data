package com.roy.hdfs.group;

import com.roy.hdfs.index.Word;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Combiner extends Reducer<Word, NullWritable, Word, NullWritable> {

    @Override
    protected void reduce(Word word, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
        }
    }
}
