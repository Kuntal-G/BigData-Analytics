package org.apache.flume.stat;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

/**
 * This interceptor counts flume events.
 * 
 * The count is collected by PeriodicCountingSource,
 * which publishes the count as a new event.
 * 
 */
public class CountingInterceptor implements Interceptor {

	private AtomicInteger count = new AtomicInteger();

	public int collect() {
		return count.getAndSet(0);
	}
	
	//@Override
	public void initialize() {
		InterceptorRegistry.register(CountingInterceptor.class, this);
	}

	//@Override
	public Event intercept(Event event) {
		count.incrementAndGet();
		return event;
	}

	//@Override
	public List<Event> intercept(List<Event> events) {
		count.addAndGet(events.size());
		return events;
	}

	//@Override	
	public void close() {
		InterceptorRegistry.deregister(this);
	}

	public static class Builder implements Interceptor.Builder {

		//@Override
		public void configure(Context context) {
			// NOP
		}

		//@Override
		public Interceptor build() {
			return new CountingInterceptor();
		}
		
	}
	
}
