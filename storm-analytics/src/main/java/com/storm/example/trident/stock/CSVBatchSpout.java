// Copyright (c) 2013, Think Big Analytics.
package com.storm.example.trident.stock;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

public class CSVBatchSpout implements IBatchSpout {

    // Trident batch size
    private static final int RECORDS_PER_BATCH = 10;

    // Field name collection
    final private Fields fields;

    // Filename to read and parse
    final private String filename;

    // Stream to read from
    private BufferedReader input;

    // Date format of incoming trade data.
    private SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );

    /**
     * Setup input filename and fields mapping
     * 
     * @param filename
     * @param fields
     */
    public CSVBatchSpout( final String filename, Fields fields ) {
        this.filename = filename;
        this.fields = fields;
    }

    /**
     * Upon startup, this Trident spout will open filename from the worker node.
     */
    @Override
    public void open( Map conf, TopologyContext context ) {
        try {
            input = new BufferedReader( new InputStreamReader( new GZIPInputStream( new FileInputStream( filename ) ) ) );
        } catch (Exception e) {
            e.printStackTrace();
            System.exit( -1 );
        }
    }

    /**
     * Storm will tell us when to emit a batch of data
     */
    @Override
    public void emitBatch( long batchId, TridentCollector collector ) {
        String line = null;
        try {
            for (int i = 0; i < RECORDS_PER_BATCH; i++) {
                line = input.readLine();
                parseLine( collector, line );
                
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Parse input line and emit values on the collector
     * 
     * @param collector
     *            The collector to emit values onto.
     * @param line
     *            The line to be parsed.
     */
    protected void parseLine( TridentCollector collector, String line ) {
        if (line != null && line.length() > 1 && line.charAt(0) != '#') {
            String fields[] = line.split( "," );

            Date date;
            try {
                // PARSE file: date, symbol, price, shares
                date = (Date) formatter.parse( fields[0] );
                String symbol = fields[1];
                Double price = Double.parseDouble( fields[2] );
                Long shares = Long.parseLong( fields[3] );

                // Send values on their way.
                collector.emit( new Values( date, symbol, price, shares ) );
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    /**
     * Here we would mark our batch as processed.
     */
    @Override
    public void ack( long batchId ) {

    }

    @Override
    public void close() {
        try {
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Spout configuration override: One spout instance per file (we're not doing splits on large files, one reason being that gzip files
     * are not easily splitable).
     */
    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism( 1 );
        return conf;
    }

    /**
     * Provide descriptor of fields to be emitted.
     */
    @Override
    public Fields getOutputFields() {
        return fields;
    }

}
