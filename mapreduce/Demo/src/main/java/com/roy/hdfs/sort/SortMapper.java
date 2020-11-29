package com.roy.hdfs.sort;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortWriteable, Text> {

    private SortWriteable k2 = new SortWriteable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String k1 = value.toString();
        if(StringUtils.isNotBlank(k1)) {
            String[] datas = k1.split(" ");
            k2.setWord(datas[0]);
            k2.setNum(Integer.valueOf(datas[1]));
            context.write(k2, value);
        }
    }
}
