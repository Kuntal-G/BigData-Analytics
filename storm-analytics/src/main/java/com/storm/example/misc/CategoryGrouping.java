package com.storm.example.misc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CategoryGrouping implements CustomStreamGrouping, Serializable {
	private static final Map<String, Integer> categories = ImmutableMap.of(
		"Financial", 0, 
		"Medical", 1, 
		"FMCG", 2, 
		"Electornics", 3
	);

	private int tasks = 0;

	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		tasks = targetTasks.size();
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		String category = (String) values.get(0);
		return ImmutableList.of(categories.get(category) % tasks);
	}
}