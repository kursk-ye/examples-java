package io.github.streamingwithflink.chapter6.processfunction;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import io.github.streamingwithflink.chapter5.kursk.ElecMeterSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;



public class DemoSideOut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        KeyedStream<ElecMeterReading,String> keysrc = env
                .addSource(new ElecMeterSource())
                .keyBy( r -> r.getId());

        SingleOutputStreamOperator<ElecMeterReading> monitoredReadings = keysrc
                .process(new DemoKFreezingMonitor());

        monitoredReadings
                .getSideOutput(new OutputTag<String>("lowing-value-alarms:"))
                .print();

        keysrc.print();

        env.execute();
    }
}
