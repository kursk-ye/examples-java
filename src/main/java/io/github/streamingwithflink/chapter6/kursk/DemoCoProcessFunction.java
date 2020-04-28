package io.github.streamingwithflink.chapter6.kursk;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import io.github.streamingwithflink.chapter5.kursk.ElecMeterSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/*
* 这个示例是合并两个dataStream,其中第一个 DataStream 的类型是ElecMeterReading，并且不断地产生
* 第二个数据流的类型是Tuple2，只有两个元素，且元素的第一个属性是id，与上一个 DataStream 的id相关联
* 第二个数据流作为控制第一个数据流使用，第二个数据流里的元素，第一个属性是id，第二个属性是截流时间周期，当收到该元素后，从当前机器时间延迟该截流时间周期，
* 第一个数据流的对应ID的流将停止输出。
* 该示例中，MID-1 在20秒后停止输出， MID-2在60秒后停止输出
* */


public class DemoCoProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple2<String,Long>> filter = env
                .fromElements(
                        Tuple2.of("MID-1",20_000L),
                        Tuple2.of("MID-2",60_000L)
                        );

        DataStream<ElecMeterReading> elecSrc = env
                .addSource(new ElecMeterSource());

        DataStream<ElecMeterReading> forward = elecSrc
                .connect(filter)
                .keyBy( r -> r.getId(), s -> s.f0)
                .process(new DemoReadingFilter());

        forward.print();

        env.execute();

    }
}

class DemoReadingFilter extends CoProcessFunction<ElecMeterReading , Tuple2<String, Long>, ElecMeterReading>{
    private ValueState<Boolean> switcher;
    private ValueState<Long> disableTimer;

    @Override
    public void open(Configuration parameters) throws Exception {
        switcher = this.getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("filterSwitch", Types.BOOLEAN));
        disableTimer = this.getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer",Types.LONG));
    }

    @Override
    public void processElement1(ElecMeterReading value, Context ctx, Collector<ElecMeterReading> out) throws Exception {
        Boolean forward = switcher.value();
        if (forward != null && forward){
            out.collect(value);
        }
    }



    @Override
    public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<ElecMeterReading> out) throws Exception {
        switcher.update(true);
        Long timerTimestamp = ctx.timerService().currentProcessingTime() + value.f1;
        Long curTimerTimestamp = disableTimer.value();
        if( curTimerTimestamp == null || timerTimestamp > curTimerTimestamp ){
            if ( curTimerTimestamp != null ){
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
            }

            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
            disableTimer.update(timerTimestamp);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ElecMeterReading> out) throws Exception {
        switcher.clear();
        disableTimer.clear();
    }
}
