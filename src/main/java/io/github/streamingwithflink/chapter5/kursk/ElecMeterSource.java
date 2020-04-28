package io.github.streamingwithflink.chapter5.kursk;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ElecMeterSource extends RichParallelSourceFunction<ElecMeterReading> {
    private Boolean running = true;

    @Override
    public void run(SourceContext<ElecMeterReading> ctx) throws Exception {
        Random rnd = new Random();
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        String[] MeterIds = new String[10];
        double[] ElecValues = new double[10];

        while (running){
            for(int i = 0 ; i < 3; i++){
                MeterIds[i] = "MID-" + i;
                ElecValues[i] = rnd.nextInt(99);
            }
            long curTime = Calendar.getInstance().getTimeInMillis();

            for(int i = 0 ; i < 3 ; i++){
                ctx.collect(new ElecMeterReading(MeterIds[i] , curTime , ElecValues[i]));
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
