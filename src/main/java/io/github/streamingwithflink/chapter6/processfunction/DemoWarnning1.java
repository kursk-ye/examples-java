package io.github.streamingwithflink.chapter6.processfunction;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoWarnning1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> out = env
                .addSource(new ElecMeterSource())
                .keyBy( r -> r.getId())
                .process(new DemoKeyedProcessFunction());

        out.print();

        env.execute();

    }
}
