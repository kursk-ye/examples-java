package io.github.streamingwithflink.chapter6.windowoperators;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class assignTSAndWMToElecMeter implements AssignerWithPeriodicWatermarks<ElecMeterReading> {
    private Long maxTs;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        Watermark  wm = new Watermark(maxTs + 500L);
        return wm;
    }

    @Override
    public long extractTimestamp(ElecMeterReading element, long previousElementTimestamp) {
        if (previousElementTimestamp < 0){
            maxTs = element.getTimestamp();
        } else {
            if ( element.getTimestamp() > maxTs  ){
                maxTs = element.getTimestamp();
            }
        }

        return element.getTimestamp();
    }
}
