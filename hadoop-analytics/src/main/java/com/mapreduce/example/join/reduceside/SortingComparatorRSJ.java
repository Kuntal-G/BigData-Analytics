package com.mapreduce.example.join.reduceside;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * To ensure that the input to the reducer is sorted on empNo, then on sourceIndex, we need a sort comparator.
 * This will guarantee that the employee data is the first set in the values list for a key, then the salary data.
 *
 * @author kuntal
 */
public class SortingComparatorRSJ extends WritableComparator {

    protected SortingComparatorRSJ() {
        super(CompositeKeyWritableRSJ.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        // Sort on all attributes of composite key
        CompositeKeyWritableRSJ key1 = (CompositeKeyWritableRSJ) w1;
        CompositeKeyWritableRSJ key2 = (CompositeKeyWritableRSJ) w2;

        int cmpResult = key1.getJoinKey().compareTo(key2.getJoinKey());
        if (cmpResult == 0)// same joinKey
        {
            return Double.compare(key1.getTag().get(), key2.getTag().get());
        }
        return cmpResult;
    }
}