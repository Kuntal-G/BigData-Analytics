package com.mapreduce.example.join.reduceside;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * In the setup method of the reducer (called only once for the task)- We are checking if the side data, a map file
 * with department data is in the distributed cache and if found, initializing the map file reader
 *
 * In the reduce method, -
 * While iterating through the value list -
 *  1. If the data is employee data (sourceIndex=1), we are looking up the department name in the map file with the
 *     deptNo, which is the last attribute in the employee data, and appending the department name to the employee data.
 *  2. If the data is historical salary data, we are only emitting salary where the last attribute is '9999-01-01'.
 *
 * Key point-
 * We have set the sort comparator to sort on empNo and sourceIndex.
 * The sourceIndex of employee data is lesser than salary data - as set in the driver.
 * Therefore, we are assured that the employee data is always first followed by salary data.
 * So for each distinct empNo, we are iterating through the values, and appending the same and emitting as output.
 *
 * @author kuntal
 */
public class ReducerRSJ extends Reducer<CompositeKeyWritableRSJ, Text, NullWritable, Text> {

    StringBuilder reduceValueBuilder = new StringBuilder("");
    NullWritable nullWritableKey = NullWritable.get();
    Text reduceOutputValue = new Text("");
    String strSeparator = ",";
    private MapFile.Reader deptMapReader = null;
    Text txtMapFileLookupKey = new Text("");
    Text txtMapFileLookupValue = new Text("");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /*
            Get side data from the distributed cache
         */
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheArchives(context.getConfiguration());

        for (Path eachPath : cacheFilesLocal) {

            if (eachPath.getName().trim().equals("depts.map")) {
                System.out.println("found departments map file for lookups");
                URI uriUncompressedFile = new File(eachPath.toString()).toURI();
                initializeDepartmentsMap(uriUncompressedFile, context);
            } else {
                System.out.println("cannot locate department map file");
            }
        }
    }

    @SuppressWarnings("deprecation")
    private void initializeDepartmentsMap(URI uriUncompressedFile, Context context) throws IOException {
        /*
            Initialize the reader of the map file (side data)
         */
        FileSystem dfs = FileSystem.get(context.getConfiguration());
        try {
            deptMapReader = new MapFile.Reader(dfs, uriUncompressedFile.toString(), context.getConfiguration());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private StringBuilder buildOutputValue(CompositeKeyWritableRSJ key, StringBuilder reduceValueBuilder, Text value) {
        if (key.getTag().get() == 1) {
            // Employee data
            /*
                Get the department name from the MapFile in distributedCache
                Insert the joinKey (empNo) to beginning of the stringBuilder
             */
            reduceValueBuilder.append(key.getJoinKey().toString()).append(strSeparator);

            String arrEmpAttributes[] = value.toString().split(",");
            txtMapFileLookupKey.set(arrEmpAttributes[3]);
            try {
                deptMapReader.get(txtMapFileLookupKey, txtMapFileLookupValue);
            } catch (Exception e) {
                txtMapFileLookupValue.set("");

            } finally {
                txtMapFileLookupValue
                      .set((txtMapFileLookupValue.equals(null) || txtMapFileLookupValue.equals("")) ? "NOT-FOUND"
                            : txtMapFileLookupValue.toString());
            }

            /*
                Append the department name to the map values to form a complete CSV of employee attributes
             */
            reduceValueBuilder.append(value.toString()).append(strSeparator)
                  .append(txtMapFileLookupValue.toString())
                  .append(strSeparator);
        } else if (key.getTag().get() == 2) {
            // Current recent salary data (1..1 on join key)
            // Salary data; Just append the salary, drop the effective-to-date
            String arrSalAttributes[] = value.toString().split(",");
            reduceValueBuilder.append(arrSalAttributes[0]).append(strSeparator);
        } else // key.getsourceIndex() == 3; Historical salary data
        {
            /*
                Get the salary data but extract only current salary (to_date='9999-01-01')
                Cardinality 1..many
             */
            String arrSalAttributes[] = value.toString().split(",");
            if (arrSalAttributes[1].equals("9999-01-01")) {
                // Salary data; Just append
                reduceValueBuilder.append(arrSalAttributes[0]).append(strSeparator);
            }
        }

        // Reset
        txtMapFileLookupKey.set("");
        txtMapFileLookupValue.set("");

        return reduceValueBuilder;
    }

    @Override
    public void reduce(CompositeKeyWritableRSJ key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        List<String> vals = new ArrayList<String>();

        // Iterate through values; First set is csv of employee data second set is salary data; The data is already
        // ordered by virtue of secondary sort; Append each value;
        for (Text value : values) {
            vals.add(value.toString());

            buildOutputValue(key, reduceValueBuilder, value);
        }

        // Drop last comma, set value, and emit output
        if (reduceValueBuilder.length() > 1) {
            reduceValueBuilder.setLength(reduceValueBuilder.length() - 1);
            // Emit output
            reduceOutputValue.set(reduceValueBuilder.toString());
            context.write(nullWritableKey, reduceOutputValue);
        } else {
            System.out.println("Key=" + key.getJoinKey().toString() + "src=" + key.getTag().get());
        }

        // Reset variables
        reduceValueBuilder.setLength(0);
        reduceOutputValue.set("");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (deptMapReader != null)
            deptMapReader.close();
    }
}
