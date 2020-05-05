package io.github.streamingwithflink.chapter7.querystate;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AvgElecValueDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        DataStream result = env.addSource(new ElecMeterSource())
                            .keyBy(r -> r.getId())
                            .flatMap( new AvgElecValue());

        //result.print();

        env.execute();
    }
}
