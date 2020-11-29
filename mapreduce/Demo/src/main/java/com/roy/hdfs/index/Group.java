package com.roy.hdfs.index;

import org.apache.hadoop.io.WritableComparator;

public class Group extends WritableComparator {

    public Group() {
        super(Word.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        Word first = (Word) a;
        Word second = (Word) b;
        String key1 = first.getFileName() + first.getStr();
        String key2 = second.getFileName() + second.getStr();
        return key1.compareTo(key2);
    }
}
