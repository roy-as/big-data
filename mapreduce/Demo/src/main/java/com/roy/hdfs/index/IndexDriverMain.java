package com.roy.hdfs.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class IndexDriverMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "indexMapReduce");

        job.setJarByClass(this.getClass());

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(Word.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(Group.class); // 自定义分组
        job.setCombinerClass(IndexCombiner.class);

        job.setReducerClass(IndexCombiner.class);
        job.setOutputKeyClass(Word.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String path = args[1];
        File file = new File(path);
        if(file.exists()) {
            path = path.substring(0, path.length() - 1) + (Integer.parseInt(path.substring(path.length() - 1)) + 1);
        }
        TextOutputFormat.setOutputPath(job, new Path(path));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int flag = ToolRunner.run(new Configuration(), new IndexDriverMain(), args);
        System.exit(flag);
    }
}
