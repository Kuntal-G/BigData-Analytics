// Copyright (c) 2013, Think Big Analytics.
package com.storm.example.trident.stock;

import java.util.HashMap;
import java.util.Map;



import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/*-
 * Manage complex stock trade statistics within the Storm Trident framework.
 * Inputs: symbol, shares, price
 * Outputs: StockStats
 */
public class StockAggregator implements CombinerAggregator<Map<String, StockStats>> {

    /**
     * Convert the TridentTuple into a map object.
     */
    @Override
    public Map<String, StockStats> init( TridentTuple tuple ) {

        Map<String, StockStats> map = zero();
        final String symbol = tuple.getStringByField( "symbol" );
        final long shares = tuple.getLongByField( "shares" );
        final double price = tuple.getDoubleByField( "price" );

        map.put( symbol, new StockStats( price, shares ) );
        return map;
    }

    /**
     * Handle hash map based combine logic
     */
    @Override
    public Map<String, StockStats> combine( Map<String, StockStats> val1, Map<String, StockStats> val2 ) {
        for (Map.Entry<String, StockStats> entry : val2.entrySet()) {
            val2.put( entry.getKey(), new StockStats( val1.get( entry.getKey() ), entry.getValue() ) );
        }
        for (Map.Entry<String, StockStats> entry : val1.entrySet()) {
            if (val2.containsKey( entry.getKey() )) {
                continue;
            }
            val2.put( entry.getKey(), entry.getValue() );
        }
        return val2;
    }

    @Override
    public Map<String, StockStats> zero() {
        return new HashMap<String, StockStats>();
    }

}