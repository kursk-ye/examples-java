package io.github.streamingwithflink.chapter6.windowoperators;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import io.github.streamingwithflink.chapter5.kursk.ElecMeterSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowAssignersProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.getCheckpointConfig().setCheckpointInterval(10_000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //env.getConfig().setAutoWatermarkInterval(1_000L);
        env.setParallelism(4);

        DataStream<ElecMeterReading> reading = env
                .addSource(new ElecMeterSource());


        reading
                .keyBy(r -> r.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .process(new ElecValueAverager());

/*        reading
                .keyBy(r -> r.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .process(new CountWindowFun<ElecMeterReading>());*/

        env.execute();
    }
}


