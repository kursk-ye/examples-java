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

        Long curTimerTimestamp = currentTimer.value(); // 前一个event的时间戳
        if (prevDayElecValue == 0.0 || value.getDayElecValue() < prevDayElecValue) {
            ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);  // 删除计时器上的注册函数
            currentTimer.clear();                                             // 当前时间戳清零
        } else if (value.getDayElecValue() > prevDayElecValue && curTimerTimestamp == 0) {  // 第一个温度升高的时刻
            Long timerTs = ctx.timerService().currentProcessingTime() + 1000L;              // 如果1秒钟后该函数还是被触发，说明1秒钟内函数没有被清零—，即说明1秒钟内温度没有下降，否则会进入上个条件
            ctx.timerService().registerProcessingTimeTimer(timerTs);                        // 函数会被从计时器移除
            currentTimer.update(timerTs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("Temperature of sensor '" + ctx.getCurrentKey() + "' monotonically increased for 1 second.");
        currentTimer.clear();
    }
}
