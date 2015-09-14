package com.mapreduce.example.join.mapside;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;

/**
 * Once the values from the source files have been joined, the Mapper.map method is called, it will receive a Text
 * object for the key (the same key across joined records) and a TupleWritable that is composed of the values joined
 * from our input files for a given key. Remember we want our final output to have the join-key in the first position,
 * followed by all of joined values in one delimited String. To achieve this we have a custom mapper to put our data
 * in the correct format
 *
 * In the CombineValuesMapper we are appending the key and all the joined values into one delimited String. Here we
 * can finally see the reason why we threw the join-key away in the previous MapReduce jobs. Since the key is the first
 * position in the values for all the datasets to be joined, our mapper naturally eliminates the duplicate keys from
 * the joined datasets. All we need to do is insert the given key into a StringBuilder, then append the values contained
 * in the TupleWritable.
 *
 * @author kuntal
 */
public class CombineValuesMapper extends Mapper<Text, TupleWritable, NullWritable, Text> {

    private static final NullWritable nullKey = NullWritable.get();
    private Text outValue = new Text();
    private StringBuilder valueBuilder = new StringBuilder();
    private String separator;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        separator = context.getConfiguration().get("separator");
    }

    @Override
    protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
        valueBuilder.append(key).append(separator);
        for (Writable writable : value) {
            valueBuilder.append(writable.toString()).append(separator);
        }
        valueBuilder.setLength(valueBuilder.length() - 1);
        outValue.set(valueBuilder.toString());
        context.write(nullKey, outValue);
        valueBuilder.setLength(0);
    }
}