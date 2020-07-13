package io.github.streamingwithflink.chapter8.datahub.filesystem;

import com.aliyun.datahub.client.model.*;
import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import io.github.streamingwithflink.chapter8.datahub.DataHubBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/*
 * 此函数没有开发完成，没有达到预想的结果！
 * 因为给PutTupleDatahubFunction的构造函数传递的参数 RecordSchema schema 并初始化对应的field,在open 和 process
 * 方法中这个field的值都为null，原因是传递的参数类型 RecordSchema 没有写序列化和反序列化的方法，但是因为其继承的 RecordSchema
 * 是alibaba 开发的，采用json序列化和反序列化,而Flink采用java.io.Serializable序列化和反序列化，Flink其中如何调用的原理还不十分清楚，所以没有完成剩余任务。
 */

public class PutTupleDatahubWindowFunction<IN extends Serializable, KEY>
        extends ProcessWindowFunction<IN, PutRecordsResult, KEY, TimeWindow> {

    private DataHubBase<IN> dataHubHandler;

    @Override
    public void open(Configuration parameters) throws Exception {
        dataHubHandler = new DataHubBase("https://dh-cn-shanghai.aliyuncs.com", "PUDA") {
        };
    }

    @Override
    public void process(KEY key,
                        Context context,
                        Iterable<IN> elements,
                        Collector<PutRecordsResult> out) {

        int subId = getRuntimeContext().getIndexOfThisSubtask();

        Map<String, String> attributeMap = new HashMap<String, String>();
        attributeMap.put("type", "PoJoElecMeterReading");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("id", FieldType.STRING));
        schema.addField(new Field("timestamp", FieldType.TIMESTAMP));
        schema.addField(new Field("dayelecvalue", FieldType.DOUBLE));

        PutRecordsResult result = null;
        try {
            result = dataHubHandler.putTupleRecords("yecustomproject3",
                    "kmg_tuple",
                    elements,
                    schema,
                    attributeMap,
                    PoJoElecMeterReading.class,
                    subId);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        //System.out.println("Fail count : " + result.getFailedRecordCount());

    }
}