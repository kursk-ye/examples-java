package io.github.streamingwithflink.chapter8;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class PoJoElecMeterSource extends RichParallelSourceFunction<PoJoElecMeterReading> {
    private Boolean running = true;
    private Long count = 0L;
    private int arraySize = 10_000;

    private transient ListState<Long> checkpointedCount;

    @Override
    public void run(SourceContext<PoJoElecMeterReading> ctx) throws Exception {
        Random rnd = new Random();
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        while (running && this.count < 1000) {
            String[] MeterIds = new String[arraySize];
            double[] ElecValues = new double[arraySize];

            for (int i = 0; i < arraySize; i++) {
                MeterIds[i] = "MID-" + i;
                ElecValues[i] = rnd.nextInt(99);
            }
            long curTime = Calendar.getInstance().getTimeInMillis();

            for (int i = 0; i < arraySize; i++) {
                int finalI = i;
                ctx.collect(new PoJoElecMeterReading(){{
                            this.setId(MeterIds[finalI]);
                            this.setTimestamp(curTime);
                            this.setDayelecvalue(ElecValues[finalI]);
                }});
            }

            this.count += arraySize;
        }
    }

    // 不能停止 ,不按while(running)的写法就会出问题
/*    @Override
    public void run(SourceContext<PoJoElecMeterReading> ctx) throws Exception {
        Random rnd = new Random();
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        String[] MeterIds = new String[arraySize];
        double[] ElecValues = new double[arraySize];

        for(int i=0; i< arraySize; i++){
            MeterIds[i] = "MID-" + i;
            ElecValues[i] = rnd.nextInt(99);
            long curTime = Calendar.getInstance().getTimeInMillis();

            ctx.collect(new PoJoElecMeterReading(MeterIds[i], curTime, ElecValues[i]));
            this.count = Long.valueOf(i);
        }
    }*/

    @Override
    public void cancel() {
        this.running = false;
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointedCount = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("count", Long.class));

        if (context.isRestored()) {
            for (Long count : this.checkpointedCount.get()) {
                this.count = count;
            }
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.checkpointedCount.clear();
        this.checkpointedCount.add(count);
    }
}