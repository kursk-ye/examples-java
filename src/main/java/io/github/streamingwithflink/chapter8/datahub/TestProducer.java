package io.github.streamingwithflink.chapter8.datahub;

import com.aliyun.datahub.client.model.*;
import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;

import java.util.*;

public class TestProducer {
    public static void main(String[] args) {
        DataHubBase dataHubHandler =
                new DataHubBase("https://dh-cn-shanghai.aliyuncs.com", "PUDA") {
        };

        Map<String, String> attributeMap = new HashMap<String, String>();
        attributeMap.put("type", "PoJoElecMeterReading");

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("id", FieldType.STRING));
        schema.addField(new Field("timestamp", FieldType.TIMESTAMP));
        schema.addField(new Field("dayelecvalue", FieldType.DOUBLE));

        List<PoJoElecMeterReading> list = new ArrayList<>();

        for(int i =0 ; i< 100; i++){
            int finalI = i;
            list.add(new PoJoElecMeterReading(){{
                this.setId("MID-"+ finalI);
                this.setTimestamp(Long.valueOf(finalI));
                this.setDayelecvalue(Double.valueOf(finalI));
            }});
        }

        MyIterable<PoJoElecMeterReading> elements = new MyIterable(list);

        PutRecordsResult result = null;
        try {
            result = dataHubHandler.putTupleRecords("yecustomproject3",
                    "kmg_tuple",
                    elements,
                    schema,
                    attributeMap,
                    PoJoElecMeterReading.class,
                    0);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}

class MyIterable<T> implements Iterable<T> {
    private List<T> list;

    public MyIterable(List<T> list) {
        this.list = list;
    }
    @Override
    public Iterator<T> iterator() {
        return list.iterator();
    }
}
