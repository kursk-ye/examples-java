package io.github.streamingwithflink.chapter6.kursk;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DemoKeyedProcessFunction extends KeyedProcessFunction<String, ElecMeterReading, String> {
    ValueState<Double> lastDayElecValue = this.getRuntimeContext().getState(new ValueStateDescriptor("lastTemp", Types.DOUBLE));
    ValueState<Long> currentTimer = this.getRuntimeContext().getState(new ValueStateDescriptor("timer", Types.LONG));

    @Override
    public void processElement(ElecMeterReading value, Context ctx, Collector<String> out) throws Exception {
        Double prevDayElecValue = lastDayElecValue.value();
        lastDayElecValue.update(value.getDayElecValue());

        Long curTimerTimestamp = currentTimer.value(); // ???
        if (prevDayElecValue == 0.0 || value.getDayElecValue() < prevDayElecValue) {
            ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
            currentTimer.clear();
        } else if (value.getDayElecValue() > prevDayElecValue && curTimerTimestamp == 0) {
            Long timerTs = ctx.timerService().currentProcessingTime() + 1000L;
            ctx.timerService().registerProcessingTimeTimer(timerTs);
            currentTimer.update(timerTs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("Temperature of sensor '" + ctx.getCurrentKey() + "' monotonically increased for 1 second.");
        currentTimer.clear();
    }
}
