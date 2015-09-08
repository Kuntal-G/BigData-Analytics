# flume-analytics

The aim of this project is to build  interceptors that exploit Flume's extensibility model to apply real-time analytics to data flows. Analysing data in-flight reduces response times and allows consumers to view information as it happens.

## The streaming topN example

The streaming topN example demonstrates how to use a chain of interceptors to compute a near real-time list of the 10 most popular hashtags from a continuous stream of twitter status updates.

Cloudera's `TwitterSource` is used to connect to the twitter firehose and emit events that match a set of search keywords. A series of Flume interceptors is then used to extract, count, and compute a rolling topN of the hashtags used in the status updates.

First, `HashtagRollingCountInterceptor` extracts and counts the hashtags in a sliding window style, and then `HashtagTopNInterceptor` takes the counters and computes the topN. `PeriodicEmissionSource` is a separate source that connect to `HashtagTopNInterceptor` and periodically emits the topN list.

## Steps to execute



