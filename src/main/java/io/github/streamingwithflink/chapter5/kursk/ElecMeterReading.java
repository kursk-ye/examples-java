package io.github.streamingwithflink.chapter5.kursk;

public class ElecMeterReading {
    private String id;
    private Long timestamp;
    private  Double DayElecValue;

    public ElecMeterReading(){
        this.id = "null";
        this.timestamp = Long.MIN_VALUE;
        this.DayElecValue = Double.MIN_VALUE;
    }

    public ElecMeterReading(String id , Long timestamp, Double DayElecValue){
        this.id = id;
        this.timestamp = timestamp;
        this.DayElecValue = DayElecValue;
    }

    @Override
    public String toString(){
        return  "(ElecMeterReading: " + "id: " + this.id + ", timestamp: " + this.timestamp + ", DayElecValue: " + this.DayElecValue + ")";
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getDayElecValue() {
        return DayElecValue;
    }

    public void setDayElecValue(Double dayElecValue) {
        DayElecValue = dayElecValue;
    }
}
