package org.hbase.aggregation.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Basic Hbase Aggregation Client to carry out rowCount, Min-Max,Average over a table values
 * @author kuntal
 *
 */
public class HBaseAggregationClient {
	private static final Logger LOG = Logger.getLogger(HBaseAggregationClient.class);

	
/**
 * Row Count of a table
 * @param tableName
 * @param cf
 * @throws Throwable
 */
	private static void rowCount(byte[] tableName,byte[] cf) throws Throwable{
		Configuration conf = HBaseConfiguration.create();
		AggregationClient aggregationClient = new AggregationClient(conf);
		HTable htable=new HTable(conf, tableName);
		Scan scan =  new  Scan();
		scan.addFamily(cf);
		long  rowCount = aggregationClient.rowCount( htable ,  null , scan);
		LOG.info("Row Count : "+rowCount);
		
	}

	/**
	 * Min-Max value of a Table
	 * @param tableName
	 * @param cf
	 * @throws Throwable
	 */
	private static void minMax(byte[] tableName,byte[] cf) throws Throwable{

		Configuration conf = HBaseConfiguration.create();
		AggregationClient aggregationClient = new AggregationClient(conf);
		HTable htable=new HTable(conf, tableName);
		Scan scan =  new  Scan();
		scan.addFamily(cf);
		ColumnInterpreter ci =new LongColumnInterpreter();
		long  min = aggregationClient.min(htable, ci, scan);
		LOG.info("Min Value : "+min);
		
		long  max = aggregationClient.max(htable, ci, scan);
		LOG.info("Max Value : "+max);


	}
	
	/**
	 * Summation and Average value of Table
	 * @param tableName
	 * @param cf
	 * @throws Throwable
	 */

	private static void sumAvg(byte[] tableName,byte[] cf) throws Throwable{
		Configuration conf = HBaseConfiguration.create();
		AggregationClient aggregationClient = new AggregationClient(conf);
		HTable htable=new HTable(conf, tableName);
		Scan scan =  new  Scan();
		scan.addFamily(cf);
		ColumnInterpreter ci =new LongColumnInterpreter();
		long  summation = aggregationClient.sum(htable, ci, scan);
		LOG.info("Summation : " + summation);
		
		double  average = aggregationClient.avg(htable, ci, scan);
		LOG.info("Average value : " + average);

	}
	
	
	public static void  main(String[] args)  {
		byte []  TABLE_NAME  = Bytes.toBytes("counttable");
		byte []  COLUMN_FAMILY  = Bytes.toBytes("count");
		try{
			HBaseAggregationClient.rowCount(TABLE_NAME, COLUMN_FAMILY);
			HBaseAggregationClient.minMax(TABLE_NAME, COLUMN_FAMILY);
			HBaseAggregationClient.sumAvg(TABLE_NAME, COLUMN_FAMILY);
		}catch(Throwable e){
			e.printStackTrace();
		}

	}
}
