package io.github.streamingwithflink.chapter8.redis;

import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceRedisDemo {
  public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
      env.setParallelism(2);

      DataStream<PoJoElecMeterReading> source = env.addSource(new RedisSourceFunction());

      source.print();

      env.execute();
  }
}
