package io.github.streamingwithflink.chapter8.datahub.filesystem;

import com.aliyun.datahub.client.model.PutRecordsResult;
import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.File;

import static org.apache.flink.streaming.api.functions.source.FileProcessingMode.PROCESS_CONTINUOUSLY;

public class Flink2DataHubFSDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(3600_000L);
        env.setParallelism(1);

        File tempFile = File.createTempFile("CsvReaderPojoType", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);
        PojoTypeInfo<PoJoElecMeterReading> typeInfo =
                (PojoTypeInfo<PoJoElecMeterReading>) TypeExtractor.createTypeInfo(PoJoElecMeterReading.class);
        PojoCsvInputFormat<PoJoElecMeterReading> fileFormat =
                new PojoCsvInputFormat<PoJoElecMeterReading>(
                        new Path(tempFile.toURI().toString()),
                        typeInfo,
                        new String[]{"id", "timestamp", "dayelecvalue"});

        DataStream<PoJoElecMeterReading> source = env
                .readFile(
                        fileFormat,
                        "file:///D:\\Download\\fs.csv",
                        PROCESS_CONTINUOUSLY,  // I don't idea why use 'PROCESS_ONCE' only read one line ?
                        1_000L,
                        TypeInformation.of(PoJoElecMeterReading.class)
                ).setParallelism(1);

        //source.print();

        DataStream<PutRecordsResult> out = source
                .keyBy(r -> r.getId())
                // 因为DataHub的写入是批量的，而window的操作也是批量的，所以采用window 而不是 stream。
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))
                .process(new PutTupleDatahubWindowFunction<>())
                .uid("PutTupleDatahubWindowFunction")
                .setParallelism(2);

        out.flatMap(new RichFlatMapFunction<PutRecordsResult, String>() {
            @Override
            public void flatMap(PutRecordsResult value, Collector<String> out) throws Exception {
                System.out.println("FailedRecordCount" + value.getFailedRecordCount());
            }
        });

        env.execute();
    }
}
