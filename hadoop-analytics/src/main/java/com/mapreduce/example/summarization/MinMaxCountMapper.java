package com.mapreduce.example.summarization;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {

	// Our output key and value Writables
	private Text outUserId = new Text();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();

	// This object will format the creation date string into a Date object
	private final static SimpleDateFormat frmt =new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	public void map(Object key, Text value, Context context)	throws IOException, InterruptedException {

		Map<String, String> parsed = null;//transformXmlToMap(value.toString());
		// Grab the "CreationDate" field since it is what we are finding
		// the min and max value of

		String strDate = parsed.get("CreationDate");
		// Grab the “UserID” since it is what we are grouping by

		String userId = parsed.get("UserId");
		// Parse the string into a Date object

		Date creationDate = null;
		try {
			creationDate = frmt.parse(strDate);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Set the minimum and maximum date values to the creationDate

		outTuple.setMin(creationDate);
		outTuple.setMax(creationDate);
		// Set the comment count to 1
		outTuple.setCount(1);
		// Set our user ID as the output key
		outUserId.set(userId);
		// Write out the hour and the average comment length
		context.write(outUserId, outTuple);
	}
}