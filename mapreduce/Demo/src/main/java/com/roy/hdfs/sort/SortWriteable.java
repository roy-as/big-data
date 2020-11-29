package com.roy.hdfs.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortWriteable implements WritableComparable<SortWriteable> {

    public SortWriteable() {
    }

    private String word;

    private Integer num;


    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public int compareTo(SortWriteable o) {
        int result = this.word.compareTo(o.getWord());
        if(result == 0) {
            result = o.getNum().compareTo(this.num);
        }
        return result;
    }

    /**
     * write和readFields应保持一致
     * @param output
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(this.word);
        output.writeInt(this.num);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.word = input.readUTF();
        this.num = input.readInt();
    }
}
