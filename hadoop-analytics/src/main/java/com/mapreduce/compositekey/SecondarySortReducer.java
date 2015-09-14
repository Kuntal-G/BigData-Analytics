package com.mapreduce.compositekey;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



/**
 * 
 * SecondarySortReducer 
 * 
 * Data arrive sorted to reducer.
 * 
 * @author kuntal
 *
 */ 
public class SecondarySortReducer 
   extends Reducer<CompositeKey, NaturalValue, Text, Text> {
	 
	public void reduce(CompositeKey key, 
	                   Iterable<NaturalValue> values,
			           Context context)
	   throws IOException, InterruptedException {

		// note that values are sorted.
        // apply moving average algorithm to sorted timeseries
        StringBuilder builder = new StringBuilder();
        for (NaturalValue data : values) {
             builder.append("(");
             String dateAsString = null;//DateUtil.getDateAsString(data.getTimestamp());
             double price = data.getPrice();
             builder.append(dateAsString);
             builder.append(",");
             builder.append(price);        	 
             builder.append(")");
        }
        context.write(new Text(key.getStockSymbol()), new Text(builder.toString()));
	} // reduce

}


