package io.github.streamingwithflink.chapter7.statefulfunction;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import io.github.streamingwithflink.chapter5.kursk.ElecMeterSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HighValueCounterDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<ElecMeterReading> source = env.addSource(new ElecMeterSource());
        KeyedStream<ElecMeterReading,String> keyedSrc = source.keyBy(r -> r.getId());

        SingleOutputStreamOperator<JobTempCounter<Integer, Long>> out = keyedSrc
                                            .flatMap(new HighValueCounter(40.0));

        env.execute();
    }

}
