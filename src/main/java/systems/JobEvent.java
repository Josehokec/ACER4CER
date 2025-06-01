package systems;

import common.EventSchema;

public class JobEvent {
    private long timestamp;
    private String jobID;
    private String eventType;
    private String username;
    private int schedulingClass;
    private String jobName;

    public JobEvent(long timestamp, String jobID, String eventType, String username, int schedulingClass, String jobName) {
        this.timestamp = timestamp;
        this.jobID = jobID;
        this.eventType = eventType;
        this.username = username;
        this.schedulingClass = schedulingClass;
        this.jobName = jobName;
    }

    public JobEvent(String[] columns) {
        this.timestamp = Long.parseLong(columns[0]);
        this.jobID = columns[1];
        this.eventType = columns[2];
        this.username = columns[3];
        this.schedulingClass = Integer.parseInt(columns[4]);
        this.jobName = columns[5];
    }

    public JobEvent(byte[] record, EventSchema schema){
        String line = schema.byteEventToString(record);
        String[] fields = line.split(",");
        this.timestamp = Long.parseLong(fields[0]);
        this.jobID = fields[1];
        this.eventType = fields[2];
        this.username = fields[3];
        this.schedulingClass = Integer.parseInt(fields[4]);
        this.jobName = fields[5];
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getSchedulingClass() {
        return schedulingClass;
    }

    public void setSchedulingClass(int schedulingClass) {
        this.schedulingClass = schedulingClass;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public String toString() {
        return timestamp + "," + jobID + "," + eventType + "," + username + "," + schedulingClass + "," + jobName;
    }
}
