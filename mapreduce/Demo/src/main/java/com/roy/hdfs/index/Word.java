package com.roy.hdfs.index;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Word implements WritableComparable<Word> {

    private String fileName;

    private String str;

    private Integer count;

    private Boolean combine;

    public Word() {
    }

    public Word(String fileName, String str, Integer count) {
        this.fileName = fileName;
        this.str = str;
        this.count = count;
    }

    public Word(String fileName, String str, Integer count, Boolean combine) {
        this.fileName = fileName;
        this.str = str;
        this.count = count;
        this.combine = combine;
    }

    @Override
    public String toString() {
        return "Word{" +
                "fileName='" + fileName + '\'' +
                ", str='" + str + '\'' +
                ", count=" + count +
                '}';
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }


    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }


    public Boolean getCombine() {
        return combine;
    }

    public void setCombine(Boolean combine) {
        this.combine = combine;
    }

    @Override
    public int compareTo(Word o) {
        if(!combine) {
            String key1 = this.getFileName() + this.getStr();
            String key2 = o.getFileName() + o.getStr();
            int result = key1.compareTo(key2);
            if (result == 0) {
                result = o.getCount().compareTo(this.count);
            }
            return result;
        }else {
            return  o.getCount().compareTo(this.count);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.fileName);
        dataOutput.writeUTF(this.str);
        dataOutput.writeInt(this.count);
        dataOutput.writeBoolean(this.combine);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.fileName = dataInput.readUTF();
        this.str = dataInput.readUTF();
        this.count = dataInput.readInt();
        this.combine = dataInput.readBoolean();
    }
}
