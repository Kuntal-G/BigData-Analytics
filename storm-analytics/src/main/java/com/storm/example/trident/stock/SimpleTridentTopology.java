// Copyright (c) 2013, Think Big Analytics.
package com.storm.example.trident.stock;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

public class SimpleTridentTopology {

    private static final int DRPC_QUERY_ITERATIONS = 2000;
    private static final int DRPC_QUERY_INTERVAL = 200;
    // Data path relative to pom.xml file.
    private static final String DATA_PATH = "data/20130301.csv.gz";
    private static final int NUM_WORKERS = 3;
    private static final int LOCAL_DURATION = 20000;

    /**
     * Launch a topology that reads stock symbols and prices from a CSV data file. Query the topology with DRPC every
     * 
     * @param args
     * @throws Exception
     */
    public static void main( String[] args ) throws Exception {
        Config conf = new Config();
        // conf.setDebug( true );
        conf.setMaxSpoutPending( 20 );

        // This topology can only be run as local because it is a toy example
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( "symbolCounter", conf, buildTopology( drpc ) );

        for (int i = 0; i < DRPC_QUERY_ITERATIONS; i++) {
            System.err.println( "Result for DRPC stock volume query -> " + drpc.execute( "trades", "INTC GE AAPL" ) );
            Thread.sleep( DRPC_QUERY_INTERVAL );
        }

    }

    public static StormTopology buildTopology( LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        // Create spout to read data from CSV file and emit fields "date", "symbol", "price", "shares"
        // The Fields instance is used to name the fields the parsed output fields are mapped to.
        CSVBatchSpout spout = new CSVBatchSpout( DATA_PATH, new Fields( "date", "symbol", "price", "shares" ) );

        //
        // In this state we will save the real-time counts for each symbol
        // For this demo, we will use an in-process memory map. We could just as easily use Memcached or a NoSQL data store
        StateFactory mapState = new MemoryMapState.Factory();

        // Real-time part of the system: a Trident topology that groups by symbol and stores per-symbol counts
        TridentState tradeVolume = topology.newStream( "quotes", spout )
        //

                // Debug output of "quotes" spout
                // .each( new Fields( "date", "symbol", "price", "shares" ), new Debug() )

                // -- fields grouping by "symbol"
                .groupBy( new Fields( "symbol" ) )

                //
                // Aggregate shares by symbol projecting new field "volume"
                .persistentAggregate( mapState, new Fields( "shares" ), new Sum(), new Fields( "volume" ) );

        /**
         * Now setup a DRPC stream on top of the "quotes" stream. The DRPC stream will generate a list of symbols and then query the
         * persistent aggregate state by symbol for the volume value.
         */
        topology.newDRPCStream( "trades", drpc )
        //

                // Freaking awesome DEBUG!!!
                // This debug statement will emit stock symbols: DEBUG: [INTC GE AAPL]
                // .each( new Fields( "args" ), new Debug() )

                // state query. The input is always "args", and needs to be split into individual fields
                // Split() implements tuple.getString(0).split(" ")
                .each( new Fields( "args" ), new Split(), new Fields( "symbol" ) )
                //
                // .each( new Fields( "symbol" ), new Debug() )
                //
                .groupBy( new Fields( "symbol" ) )

                //
                // Query that persistent state that we setup earlier
                // Query key value field is "symbol" and result is projected as "volume"
                .stateQuery( tradeVolume, new Fields( "symbol" ), new MapGet(), new Fields( "volume" ) )

                // remove nulls for 'each' value in the stream (aka Filtering)
                .each( new Fields( "volume" ), new FilterNull() )

                //
                // Debug print symbol and volume values
                // .each( new Fields( "symbol", "volume" ), new Debug() )

                //
                // Project allows us to keep only the interesting fields that interest us
                .project( new Fields( "symbol", "volume" ) );

        return topology.build();
    }
}
