package io.github.streamingwithflink.chapter8.datahub.blob;

import com.aliyun.datahub.client.model.PutRecordsResult;
import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import io.github.streamingwithflink.chapter8.PoJoElecMeterSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Flink2DataHubBlobDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(3600_000L);
        env.setParallelism(4);

        /*
        RecordSchemaSer schema = new RecordSchemaSer();
        schema.addField(new Field("id", FieldType.STRING));
        schema.addField(new Field("timestamp", FieldType.TIMESTAMP));
        schema.addField(new Field("DayElecValue", FieldType.DOUBLE));
        */

        DataStream<PoJoElecMeterReading> source = env
                .addSource(new PoJoElecMeterSource())
                .uid("source")
                .setParallelism(1);

        DataStream<PutRecordsResult> out = source
                .keyBy(r -> r.getId())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10))) // 因为DataHub的写入是批量的，而window的操作也是批量的，所以采用window 而不是 stream。
                // 这里 PutDatahubFunction 构造函数传入的参数必须序列化，否则会报错
                // org.apache.flink.api.common.InvalidProgramException: The implementation of the ProcessWindowFunction is not serializable.
                // The object probably contains or references non serializable fields.
                //.process(new TestFunction());
                .process(new PutBlobDatahubFunction<>())
                .uid("PutBlobDatahubFunction")
                .setParallelism(4);

        env.execute();
    }
}
