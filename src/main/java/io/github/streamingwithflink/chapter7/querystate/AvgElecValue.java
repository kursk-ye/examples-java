package io.github.streamingwithflink.chapter7.querystate;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class AvgElecValue extends RichFlatMapFunction<ElecMeterReading, Tuple2<String, Double>> {
    private ValueState<Tuple2<String, Double>> sumState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<String, Double>> descriptor =
                new ValueStateDescriptor("sum",
                        TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
                        }));
        descriptor.setQueryable("query-name");
        sumState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(ElecMeterReading value, Collector<Tuple2<String, Double>> out) throws Exception {
        Tuple2<String, Double> sum = new Tuple2<>("",0.0);

        if (sumState.value() == null || sumState.value().f1 == null) {
            sum.f0 = "";
            sum.f1 = 0.0;
        } else {
            sum.f0 = value.getId();
            sum.f1 += value.getDayElecValue();
        }

        sumState.update(sum);
        out.collect(sum);
    }
}
