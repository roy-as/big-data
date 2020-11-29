package com.roy.hdfs.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<Order, Text> {


    @Override
    public int getPartition(Order order, Text text, int countReduceTask) {
        String orderId = order.getOrderId();
        return (orderId.hashCode() & 2147483647) % countReduceTask;
    }
}
