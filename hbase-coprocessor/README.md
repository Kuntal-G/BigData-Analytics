Hbase Co-Processor Example:
---------------------------

An example of Hbase Aggregation client to carry out(row count, min-max, average) values of a table.Also a region co-processor to hook value before get operation.

## Steps to run

1) Aggregation Client: 

a- Make sure Hbase is running and you have the configuration file of hbase in classpath before running this example.

b- Create table and put some data:


c- Now run the HBaseAggregationClient.


2) Region Co-Processor: 

a- Prepare a jar of this project  to be copied on all the Region Servers.

b- Modify the hbase­env.sh file on all the Region Server to include the jar file created containing the co-processor code.

c- Modify the hbase­site.xml to include the class name of the Region Observer on all the Region Servers.

		<property>
		        <name>hbase.coprocessor.region.classes</name>
		        <value>org.hbase.coprocessor.example.RegionObserverExample</value>
		</property>
    
    
d- Restart the HBase cluster.
     
e- Create a 'test-table' table.
     
		create 'test-table', 'cf1'
     
f- Run the below command to retrieve a row with a rowkey @@@GETTIME@@@ which triggers the above coprocessor to add the current time in ms to the response.

 