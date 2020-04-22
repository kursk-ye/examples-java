package io.github.streamingwithflink.chapter5.kursk;

import io.github.streamingwithflink.util.SensorReading;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoUnion {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<Tuple3<Integer,Integer,Integer>> source1 = env.fromElements(
                Tuple3.of(1,3,2) , Tuple3.of(1,7,3) , Tuple3.of(2,2,4) , Tuple3.of(2,1,4) , Tuple3.of(1,5,6) , Tuple3.of(2,3,5), Tuple3.of(1,10,6), Tuple3.of(1,12,6)
        );

        DataStream<Tuple3<Integer,Integer,Integer>> source2 = env.fromElements(
                Tuple3.of(3,3,2) , Tuple3.of(4,7,3) , Tuple3.of(3,2,4) , Tuple3.of(4,1,4) , Tuple3.of(4,5,6) , Tuple3.of(3,3,5), Tuple3.of(3,10,6), Tuple3.of(3,12,6)
        );

        DataStream<Tuple3<Integer,Integer,Integer>> source3 = env.fromElements(
                Tuple3.of(5,3,2) , Tuple3.of(5,7,3) , Tuple3.of(6,2,4) , Tuple3.of(5,1,4) , Tuple3.of(6,5,6) , Tuple3.of(6,3,5), Tuple3.of(5,10,6), Tuple3.of(5,12,6)
        );

        DataStream<Tuple3<Integer,Integer,Integer>> source4 = source3.union(source1,source2);

        source4.print();

        env.execute();

    }

}
