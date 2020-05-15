package io.github.streamingwithflink.chapter8.datahub.blob;

import com.aliyun.datahub.client.model.PutRecordsResult;
import io.github.streamingwithflink.chapter8.datahub.DataHubBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class PutBlobDatahubFunction<IN extends Serializable, KEY>
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
                        Collector<PutRecordsResult> out)
            throws Exception {

        Map<String, String> attributeMap = new HashMap<String, String>();
        attributeMap.put("type", "PoJoElecMeterReading");

        PutRecordsResult result = dataHubHandler
                .putBlobRecords("yecustomproject3", "kmg_blob", elements, attributeMap);

        System.out.println("Fail count : " + result.getFailedRecordCount());

    }

}


