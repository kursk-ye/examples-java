package io.github.streamingwithflink.chapter8.datahub.tuple;

import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataHubTuple2FlinkDemo {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    env.enableCheckpointing(3600_000L);
    env.setParallelism(2);

    DataStream<PoJoElecMeterReading> source =
        env.addSource(new DataHubTupleSource())
            .uid("source")
            .setParallelism(1);

    source.print();

    env.execute();
  }
}
