package com.roy.hdfs.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GroupDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "groupMapReduce");

        job.setJarByClass(this.getClass());

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(GroupMapper.class);
        job.setMapOutputKeyClass(Order.class);
        job.setMapOutputValueClass(Text.class);

        job.setGroupingComparatorClass(Grouper.class); // 自定义分组
        job.setPartitionerClass(Partition.class);// 自定义分区
        job.setNumReduceTasks(2);// 分区数量

        job.setReducerClass(GroupReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int flag = ToolRunner.run(new Configuration(), new GroupDriver(), args);
        System.exit(flag);
    }
}
