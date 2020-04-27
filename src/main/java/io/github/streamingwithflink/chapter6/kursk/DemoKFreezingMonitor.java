package io.github.streamingwithflink.chapter6.kursk;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DemoKFreezingMonitor extends ProcessFunction<ElecMeterReading , ElecMeterReading> {
    /*
     * Could not determine TypeInformation for the OutputTag type. The most common reason is forgetting to make the
     * OutputTag an anonymous inner class. It is also not possible to use generic type variables with OutputTags, such as 'Tuple2<A, B>'.
     *
     * is it a Type bug ,too ?
     * */
    OutputTag<String> freezingAlarmOutput = new OutputTag<String>("elecValue-low_alarms");

    @Override
    public void processElement(ElecMeterReading value, Context ctx, Collector<ElecMeterReading> out) throws Exception {
        if (value.getDayElecValue() < 80){
            ctx.output(freezingAlarmOutput, "low value:" + value.getId());
        }

        out.collect(value);
    }
}
