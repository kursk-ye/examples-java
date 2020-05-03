package io.github.streamingwithflink.chapter7.statefulfunction;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import io.github.streamingwithflink.chapter5.kursk.ElecMeterSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class DemoThreshold {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<ElecMeterReading> source = env.addSource(new ElecMeterSource());
        KeyedStream<ElecMeterReading,String> keyedSrc = source.keyBy(r -> r.getId());
        DataStream<Alerts<String,Double,Double>> alertsSrc = keyedSrc.flatMap(
            new TemperatureAlertFunction(30.0)
        );

        alertsSrc.print();

        env.execute();

    }
}
