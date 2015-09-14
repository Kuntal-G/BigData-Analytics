package org.apache.flume.stat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

public class PeriodicCountingSource extends AbstractSource implements EventDrivenSource, Configurable {

	private ExecutorService service;
	private int periodInMilliseconds;
	
	//@Override
	public void configure(Context context) {
		this.periodInMilliseconds = context.getInteger("period", 1000);
	}
	
	//@Override
	public synchronized void start() {
		service = Executors.newSingleThreadExecutor();
		Runnable handler = new PeriodicHandler(this, periodInMilliseconds);
		service.execute(handler);
	}

	//@Override
	public synchronized void stop() {
		// NOP
	}

	public static class PeriodicHandler implements Runnable {
		private Source source;
		private int periodInMilliseconds;

		public PeriodicHandler(Source source, int periodInMilliseconds) { 
			this.source = source;
			this.periodInMilliseconds = periodInMilliseconds;
		}
		
		private void sleep() {
			try {
				Thread.sleep(periodInMilliseconds);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return;
			}
		}
		
		private void publish(int count) {
			Map<String, String> headers = new HashMap<String, String>();
			headers.put("count", count + "");
			Event event = EventBuilder.withBody(new byte[0], headers);
			source.getChannelProcessor().processEvent(event);
		}
		
		//@Override
		public void run() {
			while(true) {
				sleep();
				int count = 0;
				for(CountingInterceptor i : InterceptorRegistry.getInstances(CountingInterceptor.class)) {
					count += i.collect();
				}
				publish(count);
			}
		}

	}



}
