package io.github.streamingwithflink.chapter5.kursk;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class DemoConnect2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer,Long>> inputStream1 = env.fromElements(
                Tuple2.of(1,10L) ,Tuple2.of(2,8L) , Tuple2.of(1,190L) ,Tuple2.of(2,17L) ,Tuple2.of(1,100L)
                );

        DataStream<Tuple2<Integer,String >> inputStream2 = env.fromElements(
                Tuple2.of(3,"abc") ,Tuple2.of(3,"def") , Tuple2.of(4,"hij") ,Tuple2.of(3,"klm") ,Tuple2.of(4,"nop")
        );

        ConnectedStreams<Tuple2<Integer,Long> , Tuple2<Integer,String >> conn = inputStream1.connect(inputStream2);

        ConnectedStreams<Tuple2<Integer, Long>, Tuple2<Integer, String>> key1 =  conn.keyBy(0,0);

        
    }
}
