package com.mapreduce.example.join.reduceside;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class is needed to indicate the group by attribute - the natural join key of empNo
 *
 * @author kuntal
 */
public class GroupingComparatorRSJ extends WritableComparator {
    protected GroupingComparatorRSJ() {
        super(CompositeKeyWritableRSJ.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        // The grouping comparator is the joinKey (Employee ID)
        CompositeKeyWritableRSJ key1 = (CompositeKeyWritableRSJ) w1;
        CompositeKeyWritableRSJ key2 = (CompositeKeyWritableRSJ) w2;
        return key1.getJoinKey().compareTo(key2.getJoinKey());
    }
}