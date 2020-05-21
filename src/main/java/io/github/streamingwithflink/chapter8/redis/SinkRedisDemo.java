package io.github.streamingwithflink.chapter8.redis;

import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import io.github.streamingwithflink.chapter8.PoJoElecMeterSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkRedisDemo {
  public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
      env.setParallelism(2);

      DataStream<PoJoElecMeterReading> source = env.addSource(new PoJoElecMeterSource())
                                                    .uid("source")
                                                    .setParallelism(1);

      source.addSink(new RedisSinkFunction())
            .uid("sinkRedis")
            .setParallelism(2);


      env.execute();
  }
}
