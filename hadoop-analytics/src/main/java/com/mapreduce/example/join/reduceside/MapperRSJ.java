package com.mapreduce.example.join.reduceside;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * In the setup method of the mapper-
 * 1. Get the filename from the input split, cross reference it against the configuration (set in driver), to derive the
 * source index.
 *  [Driver code: Add configuration [key=filename of employee,value=1],
 *      [key=filename of current salary dataset,value=2],
 *      [key=filename of historical salary dataset,value=3]
 * 2. Build a list of attributes we cant to emit as map output for each data entity
 *      The setup method is called only once, at the beginning of a map task.  So it is the logical place to to
 *      identify the source index.
 *
 * In the map method of the mapper:
 * 3. Build the map output based on attributes required, as specified in the list from #2
 *
 * Note: For salary data, we are including the "effective till" date, even though it is not required in the final
 *       output because this is common code for a 1..1 as well as 1..many join to salary data.  If the salary data is
 *       historical, we want the current salary only, that is "effective till date= 9999-01-01".
 *
 * @author kuntal
 */
public class MapperRSJ extends Mapper<LongWritable, Text, CompositeKeyWritableRSJ, Text> {

    CompositeKeyWritableRSJ ckwKey = new CompositeKeyWritableRSJ();
    Text txtValue = new Text("");
    int intSrcIndex = 0;
    StringBuilder strMapValueBuilder = new StringBuilder("");
    List<Integer> lstRequiredAttribList = new ArrayList<Integer>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        /*
            Get the source index; (employee = 1, salary = 2)
            Added as configuration in driver
         */
        FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
        intSrcIndex = Integer.parseInt(context.getConfiguration().get(fsFileSplit.getPath().getParent().getName()));

        /*
            Initialize the list of fields to emit as output based on intSrcIndex
            (1=employee, 2=current salary, 3=historical salary)
         */
        if (intSrcIndex == 1) // employee
        {
            System.out.println("Got employee file");
            lstRequiredAttribList.add(2); // FName
            lstRequiredAttribList.add(3); // LName
            lstRequiredAttribList.add(4); // Gender
            lstRequiredAttribList.add(6); // DeptNo
        } else // salary
        {
            System.out.println("Got salary file");
            lstRequiredAttribList.add(1); // Salary
            lstRequiredAttribList.add(3); // Effective-to-date (Value of 9999-01-01 indicates current salary)
        }
    }

    private String buildMapValue(String arrEntityAttributesList[]) {
        // This method returns csv list of values to emit based on data entity

        strMapValueBuilder.setLength(0);// Initialize

        // Build list of attributes to output based on source - employee/salary
        for (int i = 1; i < arrEntityAttributesList.length; i++) {
            // If the field is in the list of required output append to string builder
            if (lstRequiredAttribList.contains(i)) {
                strMapValueBuilder.append(arrEntityAttributesList[i]).append(",");
            }
        }
        if (strMapValueBuilder.length() > 0) {
            // Drop last comma
            strMapValueBuilder.setLength(strMapValueBuilder.length() - 1);
        }

        return strMapValueBuilder.toString();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (value.toString().length() > 0) {
            String arrEntityAttributes[] = value.toString().split(",");

            // set the join key (employee id) and composite key index (determines the data type)
            ckwKey.set(arrEntityAttributes[0], intSrcIndex);
            txtValue.set(buildMapValue(arrEntityAttributes));

            context.write(ckwKey, txtValue);
        }

    }
}