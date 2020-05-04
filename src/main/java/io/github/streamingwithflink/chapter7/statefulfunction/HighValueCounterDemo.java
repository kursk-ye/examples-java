package io.github.streamingwithflink.chapter7.statefulfunction;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import io.github.streamingwithflink.chapter5.kursk.ElecMeterSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HighValueCounterDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<ElecMeterReading> source = env.addSource(new ElecMeterSource());
        KeyedStream<ElecMeterReading,String> keyedSrc = source.keyBy(r -> r.getId());

        DataStream<Tuple2<String , Double>> str2 = env.fromElements(new Tuple2<>());

        // 找不到合适的stream类型，其有方法执行HighValueCounter 或其父类的操作

        /*DataStream<Alerts<String,Double,Double>> alertsSrc = keyedSrc
                .connect(str2)
                .process( new HighValueCounter(30.0));

        alertsSrc.print();*/

        env.execute();
    }

}
