package io.github.streamingwithflink.chapter6.windowoperators;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

class ElecValueAverager extends ProcessWindowFunction<ElecMeterReading, ElecAvge, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<ElecMeterReading> elements, Collector<ElecAvge> out) throws Exception {
        double avg = 0.0;
        double wsum = 0.0;
        int wsize = 0;
        String id = "";

        for(ElecMeterReading e: elements){
            wsum = e.getDayElecValue()+wsum;
            wsize++;
            id = e.getId();
        }

        avg = wsum / wsize;

        ElecAvge avgOut = new ElecAvge(id,avg,wsize);

        out.collect(avgOut);
        System.out.println(avgOut.toString());
    }
}
