package io.github.streamingwithflink.chapter8.findExceptions;

import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import io.github.streamingwithflink.chapter8.datahub.tuple.DataHubTupleSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo {
  public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
      /**
       * 因为datahub的记录之间存在依赖关系，所以不能并行执行，必须将并行度设置为 1
       */
      env.setParallelism(1);

      DataStream<PoJoElecMeterReading> source =
              env.addSource(new DataHubTupleSource()).uid("datahubsource");

      DataStream<String> sink = source.flatMap(new RdsMapper());

      sink.print();

      env.execute();
  }
}
