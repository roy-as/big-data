package com.roy.hdfs.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WorldCountDriver extends Configured implements Tool {


    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "worldCount");

        // 运行在yarn集群上必须配置
        job.setJarByClass(this.getClass());

        // 设置输入文件读取格式
        job.setInputFormatClass(TextInputFormat.class);
        // 设置输入文件的path
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(WorldCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WorldCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输出文件格式
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输出文件path，最后一级目录不能存在
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // 是否等待执行完成
        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int flag = ToolRunner.run(new Configuration(), new WorldCountDriver(), args);
        System.exit(flag);
    }
}
