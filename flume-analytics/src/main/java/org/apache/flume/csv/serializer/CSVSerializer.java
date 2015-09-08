package org.apache.flume.csv.serializer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.apache.flume.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

public class CSVSerializer implements EventSerializer {

private final static Logger logger = LoggerFactory.getLogger(CSVSerializer.class);
	
	public static final String FORMAT = "format";
	public static final String REGEX = "regex";
	public static final String REGEX_ORDER = "regexorder";
	
	private final String DEFAULT_FORMAT = "CSV";
	private final String DEFAULT_REGEX = "(.*)";
	private final String DEFAULT_ORDER = "1";
	
	private final String format;
	private final Pattern regex;
	private final String[] regexOrder;
	private final OutputStream out;
	private Map<Integer, ByteBuffer > orderIndexer;
	
	private CSVSerializer(OutputStream out, Context ctx) {
		this.format = ctx.getString(FORMAT, DEFAULT_FORMAT);
		if (!format.equals(DEFAULT_FORMAT)){
			logger.warn("Unsupported output format" + format + ", using default instead");
		}
		this.regex = Pattern.compile(ctx.getString(REGEX, DEFAULT_REGEX));
		this.regexOrder = ctx.getString(REGEX_ORDER, DEFAULT_ORDER).split(" ");
		this.out = out;
		orderIndexer = new HashMap<Integer, ByteBuffer>();
	}

	
	public void afterCreate() throws IOException {
		// noop
	}

	
	public void afterReopen() throws IOException {
		// noop
	}

	
	public void beforeClose() throws IOException {
		// noop
	}

	
	public void flush() throws IOException {
		// noop
	}

	
	public boolean supportsReopen() {
		return true;
	}

	
	public void write(Event event) throws IOException {
		Matcher matcher = regex.matcher(new String(event.getBody(),Charsets.UTF_8));
		if (matcher.find()) {
			// first write out the timestamp
			String timestamp = event.getHeaders().get(TimestampInterceptor.Constants.TIMESTAMP);
			if (timestamp == null || timestamp.isEmpty()){
				long now = System.currentTimeMillis();
				timestamp = Long.toString(now);
			}
			out.write(timestamp.getBytes());
			out.write(',');
			// next save the regex group matches into a hash for reodering
			int groupIndex = 0;
			int totalGroups = matcher.groupCount();
			for (int i = 0, count = totalGroups; i < count; i++) {
				groupIndex = i + 1;
				orderIndexer.put(Integer.valueOf(regexOrder[i]), ByteBuffer.wrap(matcher.group(groupIndex).getBytes()));
			}
			// write out the columns of the table
			int i = 1;
			for(Integer key : orderIndexer.keySet()){
				out.write(orderIndexer.get(key).array());
				if (i < totalGroups){
					out.write(',');
				}
				i++;
			}
			out.write('\n');
		}
		else {
			logger.warn("Message skipped, no regex match: " + event.getBody().toString());
		}
	}
	
	
	public static class Builder implements EventSerializer.Builder {
		
		public EventSerializer build(Context context, OutputStream out) {
			CSVSerializer s = new CSVSerializer(out, context);
			return s;
		}
	}


}
