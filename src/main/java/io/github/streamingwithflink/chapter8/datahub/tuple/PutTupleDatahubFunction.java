package io.github.streamingwithflink.chapter8.datahub.tuple;

import com.aliyun.datahub.client.model.PutRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import io.github.streamingwithflink.chapter8.datahub.DataHubBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/*
 * 此函数没有开发完成，没有达到预想的结果！
 * 因为给PutTupleDatahubFunction的构造函数传递的参数 RecordSchema schema 并初始化对应的field,在open 和 process
 * 方法中这个field的值都为null，原因是传递的参数类型 RecordSchema 没有写序列化和反序列化的方法，但是因为其继承的 RecordSchema
 * 是alibaba 开发的，采用json序列化和反序列化,而Flink采用java.io.Serializable序列化和反序列化，Flink其中如何调用的原理还不十分清楚，所以没有完成剩余任务。
 */

public class PutTupleDatahubFunction<IN extends Serializable, KEY>
        extends ProcessWindowFunction<IN, PutRecordsResult, KEY, TimeWindow> {

    private DataHubBase dataHubHandler;
    private List<RecordEntry> recordEntries;
    private RecordSchema  schema;

    /*
     * 为了将被操作元素的数据结构和操作(PutDatahubFunction)分离，所以外部调用PutDatahubFunction时，
     * 需要将被操作元素的数据结构传入(RecordSchema)
     * 为什么不反射传入元素的class,然后由PutDatahubFunction自定义RecordSchema ?
     * 因为DataHub的 FieldType 是自定义的，和Java的原始数据类型没有关系，例如
     * schema.addField(new Field("field1", FieldType.STRING));
     * 这里的FieldType.STRING是datahub定义的枚举类型，无法反射。
     */
    public PutTupleDatahubFunction(RecordSchema pojoSchema) throws IOException {
        schema = pojoSchema;
        System.out.println("-------------- field size -------------- ? " + schema.getFields().size());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataHubHandler = new DataHubBase("https://dh-cn-shanghai.aliyuncs.com","PUDA") {
        };
        recordEntries = new ArrayList<>();


    }

    @Override
    public void process(KEY KEY,
                        Context context,
                        Iterable<IN> elements,
                        Collector<PutRecordsResult> out)
            throws Exception {

        RecordEntry entry = new RecordEntry();


        for (IN e : elements) {

/*            TupleRecordData tupleData = new TupleRecordData(this.schema.value()) {{
                Field[] inputFields = e.getClass().getDeclaredFields();
                for (Field f : inputFields) {
                    try {
                        Object value = e.getFiledValueByName(f.getName());

*//*                        System.out.println("---------------------------");
                        System.out.println("name:" + f.getName() + " value: " + value);*//*

                        this.setField(f.getName(), value);
                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
            }};*/
/*            entry.setRecordData(data);
            recordEntries.add(entry);*/
        }

/*        PutRecordsByShardResult result = dataHubHandler
                .putRecordsByShard("yecustomproject3", "kmg_tuple", "0", recordEntries);*/

    }
}