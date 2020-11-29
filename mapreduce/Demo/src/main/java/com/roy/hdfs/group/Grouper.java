package com.roy.hdfs.group;

import org.apache.hadoop.io.WritableComparator;


public class Grouper extends WritableComparator {


    public Grouper() {
        super(Order.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        Order first = (Order) a;
        Order second = (Order) b;
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
