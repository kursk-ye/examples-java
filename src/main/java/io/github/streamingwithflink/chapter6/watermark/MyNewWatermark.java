package io.github.streamingwithflink.chapter6.watermark;

import io.github.streamingwithflink.chapter5.kursk.ElecMeterReading;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

public class MyNewWatermark implements AssignerWithPeriodicWatermarks<ElecMeterReading> {
    private Long maxTs = Long.MIN_VALUE;
    private Long bound = 60L*1000;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        Watermark wm = new Watermark(maxTs - bound);

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();

        //String result1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(wm.getTimestamp() * 1000));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String result1 = sdf.format(new Date(wm.getTimestamp()));

        System.out.println("current " +dtf.format(now) + " and watermark's ts is " + result1 );

        return wm;
    }

    @Override
    public long extractTimestamp(ElecMeterReading element, long previousElementTimestamp) {
        Long ts = previousElementTimestamp;
        maxTs = Long.max(ts, maxTs);
        return maxTs;
    }
}
