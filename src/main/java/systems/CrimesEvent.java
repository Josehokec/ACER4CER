package systems;

import common.EventSchema;

public class CrimesEvent {
    private String primaryType;
    private int id;
    private int beat;
    private int district;
    private double latitude;
    private double longitude;
    private long timestamp;

    public CrimesEvent(String primaryType, int id, int beat, int district, double latitude, double longitude, long timestamp) {
        this.primaryType = primaryType;
        this.id = id;
        this.beat = beat;
        this.district = district;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
    }

    public CrimesEvent(String[] columns){
        if(columns.length != 7){
            throw new RuntimeException("length can not match");
        }
        this.primaryType = columns[0];
        this.id = Integer.parseInt(columns[1]);
        this.beat = Integer.parseInt(columns[2]);
        this.district = Integer.parseInt(columns[3]);
        this.latitude = Double.parseDouble(columns[4]);
        this.longitude = Double.parseDouble(columns[5]);
        this.timestamp = Long.parseLong(columns[6]);
    }

    public CrimesEvent(byte[] records, EventSchema schema){
        String line = schema.byteEventToString(records);
        String[] columns = line.split(",");
        this.primaryType = columns[0];
        this.id = Integer.parseInt(columns[1]);
        this.beat = Integer.parseInt(columns[2]);
        this.district = Integer.parseInt(columns[3]);
        this.latitude = Double.parseDouble(columns[4]);
        this.longitude = Double.parseDouble(columns[5]);
        this.timestamp = Long.parseLong(columns[6]);
    }

    public String getPrimaryType() {
        return primaryType;
    }

    public void setPrimaryType(String primaryType) {
        this.primaryType = primaryType;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getBeat() {
        return beat;
    }

    public void setBeat(int beat) {
        this.beat = beat;
    }

    public int getDistrict() {
        return district;
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString(){
        return primaryType + "," + id + "," + beat + "," + district + "," + latitude + "," + longitude + "," + timestamp;
    }
}

