package com.storm.example.trident.stock;

public class StockStats {

    public long count;
    public double sum;
    public double low;
    public double high;
    public double open;
    public double close;
    public long shareVolume;
    public double priceVolume;
    public double avgPrice;
    public double weightedAvgPrice;

    public StockStats() {
        init( 0.0, 0L );
    }

    public StockStats( final StockStats val1, final StockStats val2 ) {
        if (null == val1 && null == val2) {
            System.err.println( "val1 and val2 are null" );
            init( 0.0, 0L );
            return;
        } else if (null == val1 && !(null == val2)) {
            init( val2.avgPrice, val2.shareVolume );
        } else if (!(null == val1) && (null == val2)) {
            init( val1.avgPrice, val1.shareVolume );
        } else {

            count = val1.count + val2.count;
            sum = val1.sum + val2.sum;
            low = Math.min( val1.low, val2.low );
            high = Math.max( val1.high, val2.high );
            // open =
            // close =
            shareVolume = val1.shareVolume + val2.shareVolume;
            priceVolume = val1.priceVolume + val2.priceVolume;
            avgPrice = sum / count;
            weightedAvgPrice = priceVolume / shareVolume;
            // System.err.println( "count=" + count );
        }
    }

    public StockStats( final double price, final long shares ) {
        init( price, shares );
    }

    private void init( final double price, final long shares ) {
        count = 1;
        sum = price;
        low = price;
        high = price;
        open = price;
        shareVolume = shares;
        priceVolume = price * shares;
        avgPrice = price;
        weightedAvgPrice = price;
    }

    public StockStats add( final double price, final long shares ) {
        count++;
        sum += price;
        avgPrice = (sum / count);
        low = Math.min( low, price );
        high = Math.max( high, price );
        close = price;
        shareVolume += shares;
        priceVolume += price * shares;
        weightedAvgPrice = priceVolume / shareVolume;
        return this;
    }

    @Override
    public String toString() {
        return String.format(
                "{count:%d,sum:%f,avgPrice%f,low:%f,high:%f,open:%f,close:%f,shareVolume:%d,priceVolume:%f,weightedAvgPrice:%f}", count,
                sum, avgPrice, low, high, open, close, shareVolume, priceVolume, weightedAvgPrice );
    }

}