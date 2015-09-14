package org.apache.flume.stat;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flume.interceptor.Interceptor;

public class InterceptorRegistry {
	
	private static Map<Class<?>, Set<Object>> interceptors = Collections.synchronizedMap(new HashMap<Class<?>, Set<Object>>());
	
	public static <T extends Interceptor> void register(Class<T> type, T instance) {
		if(type == null)
			throw new IllegalArgumentException("Type may not be null");
		if(instance == null)
			throw new IllegalArgumentException("Instance may not be null");
		if(!interceptors.containsKey(type)) {
			interceptors.put(type, Collections.synchronizedSet(new HashSet<Object>()));
		}
		interceptors.get(type).add(instance);
	}
	
    @SuppressWarnings("unchecked")
	public static <T extends Interceptor> Set<T> getInstances(Class<T> type) {
    	if(interceptors.containsKey(type)) {
    		return ((Set<T>) interceptors.get(type));
    	} else {
    		return Collections.emptySet();
    	}
    }
	
    public static <T extends Interceptor> void deregister(T instance) {
    	interceptors.get(instance.getClass()).remove(instance);
    }
    
    public static void clear() {
    	interceptors.clear();
    }
}
