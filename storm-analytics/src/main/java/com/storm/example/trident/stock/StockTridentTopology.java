// Copyright (c) 2013, Think Big Analytics.
package com.storm.example.trident.stock;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

/*-
 * Count the number of stock trades, by stock symbol.
 * Query a subset of the stock symbols.
 */
public class StockTridentTopology {

    private static final String DATA_PATH = "data/20130301.csv.gz";
    private static final int NUM_WORKERS = 3;
    private static final int LOCAL_DURATION = 20000;

    public static void main( String[] args ) throws Exception {
        Config conf = new Config();
        // conf.setDebug( true );
        conf.setMaxSpoutPending( 200 );

        // This topology can only be run as local because it is a toy example
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( "StockTridentTopology", conf, buildTopology( drpc ) );

        // Setup DRPC query to ask for results on just 3 stocks
        final String symbols = "GE INTC AAPL";
        for (int i = 0; i < 2000; i++) {
            System.err.printf( "Result for symbols -> '%s': %s\n", symbols, drpc.execute( "trades", symbols ) );
            Thread.sleep( 1000 );
        }
    }

    public static StormTopology buildTopology( LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();
        CSVBatchSpout spout = new CSVBatchSpout( DATA_PATH, new Fields( "date", "symbol", "price", "shares" ) );

        // In this state we will save the real-time counts for each symbol
        StateFactory mapState = new MemoryMapState.Factory();

        // Real-time part of the system: a Trident topology that groups by symbol and stores per-symbol counts
        TridentState tradeCount = topology.newStream( "quotes", spout )
        //
                .groupBy( new Fields( "symbol" ) )
                // count(symbol) AS count
                .persistentAggregate( mapState, new Fields( "symbol" ), new Count(), new Fields( "count" ) );

        topology.newDRPCStream( "trades", drpc )
        // state query
                .each( new Fields( "args" ), new Split(), new Fields( "symbol" ) )
                //
                .groupBy( new Fields( "symbol" ) )
                //
                .stateQuery( tradeCount, new Fields( "symbol" ), new MapGet(), new Fields( "count" ) )
                //
                .each( new Fields( "count" ), new FilterNull() )
                //
                // Project allows us to keep only the interesting results
                .project( new Fields( "symbol", "count" ) );

        // sum(count(*)) as sumTotal
        // .aggregate( new Fields( "count" ), new Sum(), new Fields( "sumTotal" ) );

        return topology.build();
    }
}
