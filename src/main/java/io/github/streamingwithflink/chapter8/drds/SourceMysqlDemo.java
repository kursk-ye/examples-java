package io.github.streamingwithflink.chapter8.drds;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 失败，未完成Flink将mysql作为源的功能，代码报错： Caused by: java.lang.StackOverflowError at
 * com.esotericsoftware.kryo.Generics.getConcreteClass(Generics.java:43) at
 * com.esotericsoftware.kryo.Generics.getConcreteClass(Generics.java:44) at
 * com.esotericsoftware.kryo.Generics.getConcreteClass(Generics.java:44) at
 * com.esotericsoftware.kryo.Generics.getConcreteClass(Generics.java:44) at
 * com.esotericsoftware.kryo.Generics.getConcreteClass(Generics.java:44) at
 * com.esotericsoftware.kryo.Generics.getConcreteClass(Generics.java:44) at
 * com.esotericsoftware.kryo.Generics.getConcreteClass(Generics.java:44) at
 * com.esotericsoftware.kryo.Generics.getConcreteClass(Generics.java:44)
 */
public class SourceMysqlDemo {
  public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
      env.setParallelism(2);

      DataStream<ElecMeterReading> source =  env.addSource( new MysqlSourceFunction())
                                                    .uid("source")
                                                    .setParallelism(1);

      source.print();

      env.execute();
  }
}
