package io.github.streamingwithflink.chapter5.kursk;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ElecMeterTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<ElecMeterReading> {

    public ElecMeterTimeAssigner(){
        super(Time.seconds(5));
    }

    @Override
    public long extractTimestamp(ElecMeterReading element) {
        return element.getTimestamp();
    }
}
