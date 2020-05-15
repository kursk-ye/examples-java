package io.github.streamingwithflink.chapter8.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.model.*;
import io.github.streamingwithflink.chapter8.CommonUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public abstract class DataHubBase<IN> {

    private String TEST_ENDPOINT;
    private transient String TEST_AK;
    private transient String TEST_SK;

    private String ODPS_ENDPOINT = "** odps endpoint **";
    private String ODPS_TUNNEL_ENDPOINT = "** tunnel endpoint **";
    private String ODPS_PROJECT = "** odps project **";
    private String ODPS_TABLE = "** odps table **";

    private String ADS_HOST = "** ads hot**";
    private int ADS_PORT = 9999;
    private String ADS_USER = "** ads user **";
    private String ADS_PASSWORD = "** ads password **";
    private String ADS_DATABASE = "** ads database **";
    private String ADS_TABLE = "** ads table **";

    private boolean enablePb = false;
    private DatahubClient client;
    private String AppName;

    public DataHubBase(String TEST_ENDPOINT, String AppName) {
        if (TEST_ENDPOINT == "") {
            this.TEST_ENDPOINT = "https://dh-cn-shanghai.aliyuncs.com";
        } else {
            this.TEST_ENDPOINT = TEST_ENDPOINT;
        }

        this.TEST_AK = "***";
        this.TEST_SK = "***";
        createClient();

        this.AppName = AppName;
    }

    private void createClient() {
        this.client = DatahubClientBuilder.newBuilder().setDatahubConfig(
                new DatahubConfig(this.getTestEndpoint(), new AliyunAccount(this.TEST_AK, this.TEST_SK), enablePb)
        ).build();
    }

    /*
     * 将一个Iterable对象一次性写入Datahub Blob shard
     */
    public PutRecordsResult putBlobRecords(String projectName,
                                           String topicName,
                                           Iterable<IN> elements,
                                           Map<String, String> attributeMap)
            throws IOException {

        List<RecordEntry> recordEntries = new ArrayList<>();

        for (IN e : elements) {
            BlobRecordData data = new BlobRecordData(CommonUtils.convertObjToBytesArray(e));
            RecordEntry entry = new RecordEntry();

            for (Map.Entry<String, String> en : attributeMap.entrySet()) {
                entry.addAttribute(en.getKey(), en.getValue());
            }

            entry.setRecordData(data);
            recordEntries.add(entry);
        }

        return client.putRecords(projectName, topicName, recordEntries);
    }

    /*
     * 将一个Iterable对象一次性写入Datahub Tuple shard
     */
    public <T> PutRecordsResult putTupleRecords(String projectName,
                                                String topicName,
                                                Iterable<IN> elements,
                                                RecordSchema schema,
                                                Map<String, String> attributeMap,
                                                Class<T> type,
                                                int subId)
            throws Throwable {
        //int count = 0;
        List<RecordEntry> recordEntries = new ArrayList<>();

        for (IN e : elements) {
            //System.out.println("subId " + subId + e.toString());
            //count++;

            TupleRecordData data = new TupleRecordData(schema) {{
                for (Field f : schema.getFields()) {
                    Object filedValue = CommonUtils.invokeGetterMethod(e, f.getName(), type);
                    setField(f.getName(), filedValue);
                }
            }};
            RecordEntry entry = new RecordEntry(); // must be in loop interval ,each element new one RecordEntry instance!
            for (Map.Entry<String, String> en : attributeMap.entrySet()) {
                entry.addAttribute(en.getKey(), en.getValue());
            }

            entry.setRecordData(data);
            recordEntries.add(entry);
        }

/*        for (RecordEntry en : recordEntries) {
            TupleRecordData data = (TupleRecordData) en.getRecordData();
            System.out.println("data:" +data.getField("id")+" " +data.getField("timestamp") + " " + data.getField("dayelecvalue"));
        }*/

        /*System.out.println("sub Id " + subId + " count is " + count);
        System.out.println("sub Id " + subId + " recordEntries size is " + recordEntries.size());*/
        return client.putRecords(projectName, topicName, recordEntries);
    }

    public PutRecordsByShardResult putRecordsByShard(String projectName, String topicName, String shardId, List<RecordEntry> records) {
        return client.putRecordsByShard(projectName, topicName, shardId, records);
    }

    public String getTestEndpoint() {
        return this.TEST_ENDPOINT;
    }

}
