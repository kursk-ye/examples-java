package io.github.streamingwithflink.chapter8;

import com.aliyun.datahub.client.model.PutRecordsResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TestFunction
        extends ProcessWindowFunction<PoJoElecMeterReading, PutRecordsResult, String, TimeWindow> {

    @Override
    public void process(String s,
                        Context context,
                        Iterable<PoJoElecMeterReading> elements,
                        Collector<PutRecordsResult> out)
            throws Exception {
        for(PoJoElecMeterReading e : elements){
            System.out.println(e.toString());
        }

    }
}
