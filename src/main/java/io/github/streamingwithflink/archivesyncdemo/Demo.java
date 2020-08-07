package io.github.streamingwithflink.archivesyncdemo;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo {
  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    env.enableCheckpointing(3_000L);

    //DataStream<OldTableA> oldTableADataSource = env.addSource()


  }
}
