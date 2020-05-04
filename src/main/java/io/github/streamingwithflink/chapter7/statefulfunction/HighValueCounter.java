package io.github.streamingwithflink.chapter7.statefulfunction;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.util.Collector;


import java.util.List;

public class HighValueCounter
        extends RichFlatMapFunction<ElecMeterReading , JobTempCounter<Integer, Long>>
        implements ListCheckpointed<Long> {

    private Integer subtaskIdx ;
    private Long highValueCnt;
    private Double threshold;

    public HighValueCounter(Double threshold){
        this.subtaskIdx= getRuntimeContext().getIndexOfThisSubtask();
        this.highValueCnt = 0L;
        this.threshold = threshold;
    }

    @Override
    public void flatMap(ElecMeterReading value, Collector<JobTempCounter<Integer, Long>> out) throws Exception {
        if(value.getDayElecValue() > this.threshold){
            highValueCnt += 1;
            out.collect(new JobTempCounter<>(this.subtaskIdx,this.highValueCnt));
        }
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
        return java.util.Collections.singletonList(this.highValueCnt);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
        this.highValueCnt = 0L;
        for(Long l : state){
            this.highValueCnt += l;
        }
    }
}

class JobTempCounter<T1,T2>{
    T1 t1;
    T2 t2;

    public JobTempCounter(T1 t1, T2 t2){
        this.t1 = t1;
        this.t2 = t2;
    }

    public T1 getT1() {
        return t1;
    }

    public void setT1(T1 t1) {
        this.t1 = t1;
    }

    public T2 getT2() {
        return t2;
    }

    public void setT2(T2 t2) {
        this.t2 = t2;
    }




}
