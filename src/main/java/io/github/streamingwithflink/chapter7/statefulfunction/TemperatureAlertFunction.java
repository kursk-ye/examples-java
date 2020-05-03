package io.github.streamingwithflink.chapter7.statefulfunction;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class TemperatureAlertFunction extends RichFlatMapFunction<ElecMeterReading , Alerts<String,Double,Double>> {
    private ValueState<Double> lastValueState;
    private Double threshold;

    public TemperatureAlertFunction(Double threshold){
        this.threshold = threshold;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.lastValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Double>("lastValueState" , Types.DOUBLE));

    }



    @Override
    public void flatMap(ElecMeterReading value, Collector<Alerts<String, Double, Double>> out) throws Exception {
        Double lastValue = lastValueState.value();
        if ( lastValue != null){
            Double diff = value.getDayElecValue() - lastValue;
            if (diff > threshold){
                out.collect(new Alerts<>("threshold is got" ,value.getDayElecValue(),diff));
            }
        }

        lastValueState.update(value.getDayElecValue());

    }
}
