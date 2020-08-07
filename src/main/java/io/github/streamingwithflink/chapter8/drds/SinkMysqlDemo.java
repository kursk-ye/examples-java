package io.github.streamingwithflink.chapter8.drds;

import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import io.github.streamingwithflink.chapter8.PoJoElecMeterSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkMysqlDemo {
  public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
      env.setParallelism(1);

      DataStream<PoJoElecMeterReading> source =  env.addSource( new PoJoElecMeterSource());


      DataStreamSink sinkstream= source.addSink(new SinkMysqlFunction());

      env.execute();
  }
}
