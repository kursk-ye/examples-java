package io.github.streamingwithflink.chapter5.kursk;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoRolling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Integer,Integer,Integer>> inputStream = env.fromElements(
                Tuple3.of(1,3,2) , Tuple3.of(1,7,3) , Tuple3.of(2,2,4) , Tuple3.of(2,1,4) , Tuple3.of(1,5,6) , Tuple3.of(2,3,5), Tuple3.of(1,10,6), Tuple3.of(1,12,6)
        );

        DataStream<Tuple3<Integer,Integer,Integer>> resultStream = inputStream
                .keyBy(0)
                .maxBy(1);

        resultStream.print();

        env.execute();
    }
}
