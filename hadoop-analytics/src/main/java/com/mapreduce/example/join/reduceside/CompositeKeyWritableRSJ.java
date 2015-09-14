package com.mapreduce.example.join.reduceside;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The composite key is a combination of the joinKey empNo, and the source Index (1=employee file.., 2=salary file...)
 *
 * @author kuntal
 */
public class CompositeKeyWritableRSJ implements Writable, WritableComparable<CompositeKeyWritableRSJ> {

    private Text joinKey = new Text();
    private IntWritable tag = new IntWritable();

    public CompositeKeyWritableRSJ() {
    }

    public void set(String key, int tag){
        this.joinKey.set(key);
        this.tag.set(tag);
    }

    @Override
    public int compareTo(CompositeKeyWritableRSJ taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if(compareValue == 0 ){
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
        return compareValue;
    }

    public static CompositeKeyWritableRSJ read(DataInput in) throws IOException {
        CompositeKeyWritableRSJ taggedKey = new CompositeKeyWritableRSJ();
        taggedKey.readFields(in);
        return taggedKey;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        joinKey.write(out);
        tag.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        joinKey.readFields(in);
        tag.readFields(in);
    }

    public Text getJoinKey() {
        return joinKey;
    }

    public IntWritable getTag() {
        return tag;
    }

    // Data members
//    private String joinKey;// EmployeeID
//    private int sourceIndex;// 1=Employee data; 2=Salary (current) data; 3=Salary historical data
//
//    public CompositeKeyWritableRSJ() {
//    }
//
//    public CompositeKeyWritableRSJ(String joinKey, int sourceIndex) {
//        this.joinKey = joinKey;
//        this.sourceIndex = sourceIndex;
//    }
//
//    @Override
//    public String toString() {
//        return (new StringBuilder().append(joinKey).append("\t").append(sourceIndex)).toString();
//    }
//
//    public void readFields(DataInput dataInput) throws IOException {
//        joinKey = WritableUtils.readString(dataInput);
//        sourceIndex = WritableUtils.readVInt(dataInput);
//    }
//
//    public void write(DataOutput dataOutput) throws IOException {
//        WritableUtils.writeString(dataOutput, joinKey);
//        WritableUtils.writeVInt(dataOutput, sourceIndex);
//    }
//
//    public int compareTo(CompositeKeyWritableRSJ objKeyPair) {
//        int result = joinKey.compareTo(objKeyPair.joinKey);
//        if (0 == result) {
//            result = Double.compare(sourceIndex, objKeyPair.sourceIndex);
//        }
//        return result;
//    }
//
//    public String getjoinKey() {
//        return joinKey;
//    }
//
//    public void setjoinKey(String joinKey) {
//        this.joinKey = joinKey;
//    }
//
//    public int getsourceIndex() {
//        return sourceIndex;
//    }
//
//    public void setsourceIndex(int sourceIndex) {
//        this.sourceIndex = sourceIndex;
//    }
}