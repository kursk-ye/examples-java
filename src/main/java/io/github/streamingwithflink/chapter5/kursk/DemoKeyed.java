package io.github.streamingwithflink.chapter5.kursk;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoKeyed {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

/*        int parallelism = env.getParallelism();
        int Maxparallelism = env.getMaxParallelism();
        System.out.println("parallelism: " + parallelism);
        System.out.println("Maxparallelism: " + Maxparallelism);*/
         env.setParallelism(1);

        DataStream<ElecMeterReading> source = env
                .addSource(new ElecMeterSource())
                .assignTimestampsAndWatermarks(new ElecMeterTimeAssigner());

        KeyedStream<ElecMeterReading, String> keyed = source
                .keyBy(r -> r.getId());

        DataStream<ElecMeterReading> maxElecValue = keyed
                .reduce( (r1 , r2) -> {
                    System.out.println("compare start:");
                    System.out.print("r1:" + r1.getId());
                    System.out.print("  timestamp:" + r1.getTimestamp());
                    System.out.println("  value:" + r1.getDayElecValue());
                    System.out.print("r2:" + r2.getId());
                    System.out.print("  timestamp:" + r2.getTimestamp());
                    System.out.println("  value:" + r2.getDayElecValue());
                    if ( r1.getDayElecValue() > r2.getDayElecValue() ){
                        return  r1;
                    }
                    else
                    {
                        return r2;
                    }
                });

        maxElecValue.print();

        env.execute("max ElecValue job");

    }


}
