package systems;

import common.EventSchema;

public class StockEvent {
    //<ticker>,<open>,<high>,<low>,<close>,<vol>,<date>
    private String ticker;
    private float open;
    private float high;
    private float low;
    private float close;
    private int vol;
    private long timestamp;

    public StockEvent(){
        ticker = "xxx";
    }

    public StockEvent(String[] columns){
        if(columns.length != 7){
            throw new RuntimeException("length can not match");
        }
        this.ticker = columns[0];
        this.open = Float.parseFloat(columns[1]);
        this.high = Float.parseFloat(columns[2]);
        this.low = Float.parseFloat(columns[3]);
        this.close = Float.parseFloat(columns[4]);
        this.vol = Integer.parseInt(columns[5]);
        this.timestamp = Long.parseLong(columns[6]);
    }

    public StockEvent(byte[] record, EventSchema schema){
        String line = schema.byteEventToString(record);
        String[] columns = line.split(",");
        this.ticker = columns[0];
        this.open = Float.parseFloat(columns[1]);
        this.high = Float.parseFloat(columns[2]);
        this.low = Float.parseFloat(columns[3]);
        this.close = Float.parseFloat(columns[4]);
        this.vol = Integer.parseInt(columns[5]);
        this.timestamp = Long.parseLong(columns[6]);
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public float getOpen() {
        return open;
    }

    public void setOpen(float open) {
        this.open = open;
    }

    public float getHigh() {
        return high;
    }

    public void setHigh(float high) {
        this.high = high;
    }

    public float getLow() {
        return low;
    }

    public void setLow(float low) {
        this.low = low;
    }

    public float getClose() {
        return close;
    }

    public void setClose(float close) {
        this.close = close;
    }

    public int getVol() {
        return vol;
    }

    public void setVol(int vol) {
        this.vol = vol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString(){
        return ticker + "," + open + "," + high + "," + low + "," + close + "," + vol + "," + timestamp;
    }
}
