package io.github.streamingwithflink.chapter6.windowoperators;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountWindowFun<IN> extends ProcessWindowFunction<IN, String, String, TimeWindow> {
    int count = 0;

    @Override
    public void process(String s, Context context, Iterable<IN> elements, Collector<String> out) throws Exception {
        int subId = getRuntimeContext().getIndexOfThisSubtask();

        for (IN e : elements) {
            this.count++;
            System.out.println("subId " + subId + e.toString());
        }

        System.out.println("subId " + subId + " count : " + this.count);

    }

    @Override
    public void close() throws Exception {


        super.close();
    }
}
