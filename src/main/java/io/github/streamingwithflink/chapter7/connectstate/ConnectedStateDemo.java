package io.github.streamingwithflink.chapter7.connectstate;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import io.github.streamingwithflink.chapter5.kursk.ElecMeterSource;
import io.github.streamingwithflink.chapter7.statefulfunction.Alerts;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class ConnectedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<ElecMeterReading> elecStream = env.addSource(new ElecMeterSource());
        DataStream<ThresholdUpdate> thresholds =
                env.fromElements(
                        new ThresholdUpdate("MID-1", 20.0), new ThresholdUpdate("MID-2", 30.0)); // 这不就是档案数据的例子么？
        KeyedStream<ElecMeterReading, String> keyedSensorData = elecStream
                .keyBy(r -> r.getId());

        MapStateDescriptor broadcastStateDescriptor =
                new MapStateDescriptor<String, Double>("thresholds", Types.STRING, Types.DOUBLE);
        BroadcastStream<ThresholdUpdate> broadcastThresholds = thresholds.broadcast(broadcastStateDescriptor);

        DataStream<Alerts<String, Double, Double>> alerts = keyedSensorData
                .connect(broadcastThresholds)
                .process(new UpdatableElecValueAlertFunction());

        alerts.print();

        env.execute();
    }
}


class UpdatableElecValueAlertFunction
        extends KeyedBroadcastProcessFunction<String, ElecMeterReading, ThresholdUpdate,
        Alerts<String, Double, Double>> {

    private MapStateDescriptor<String, Double> thresholdStateDescriptor =
            new MapStateDescriptor("thresholds", Types.STRING, Types.DOUBLE);
    private ValueState<Double> lastTempState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> lastTempDescriptor =
                new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE);
        lastTempState = getRuntimeContext().getState(lastTempDescriptor);
    }

    @Override
    public void processElement(ElecMeterReading value, ReadOnlyContext ctx,
                               Collector<Alerts<String, Double, Double>> out) throws Exception {
        ReadOnlyBroadcastState<String, Double> thresholds = ctx.getBroadcastState(thresholdStateDescriptor);
        if (thresholds.contains(value.getId())) {
            Double sensorThreshold = thresholds.get(value.getId());
            Double lastTemp = lastTempState.value();
            Double diff = Math.abs(value.getDayElecValue() - lastTemp);
            if (diff > sensorThreshold) {
                out.collect(new Alerts<>(value.getId(), diff, sensorThreshold));
            }
        }

        lastTempState.update(value.getDayElecValue());
    }

    @Override
    public void processBroadcastElement(ThresholdUpdate value, Context ctx,
                                        Collector<Alerts<String, Double, Double>> out) throws Exception {
        BroadcastState<String, Double> thresholds = ctx.getBroadcastState(thresholdStateDescriptor);

        if (value.threshold != 0.0d) {
            thresholds.put(value.id, value.threshold);
        } else {
            thresholds.remove(value.id);
        }
    }
}