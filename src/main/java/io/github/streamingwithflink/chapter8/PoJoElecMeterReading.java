package io.github.streamingwithflink.chapter8;

import java.io.Serializable;

public class PoJoElecMeterReading implements Serializable {
    private String id;
    private Long timestamp;
    private Double dayelecvalue;

/*
    只能使用不带参数的构造函数，不能自定义带参数的构造函数，否则Flink不会作为POJO，会作为generic type
    public PoJoElecMeterReading(String meterId, long curTime, double elecValue) {
        this.id = meterId;
        this.timestamp = curTime;
        this.dayelecvalue = elecValue;
    }
    */

    @Override
    public String toString() {
        return "(ElecMeterReading: " + "id: " + this.id + ", timestamp: " + this.timestamp + ", dayelecvalue: " + this.dayelecvalue + ")";
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

    public Double getDayelecvalue() {
        return dayelecvalue;
    }

    public void setDayelecvalue(Double dayelecvalue) {
        this.dayelecvalue = dayelecvalue;
    }
}
