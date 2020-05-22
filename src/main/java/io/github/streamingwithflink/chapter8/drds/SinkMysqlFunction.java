package io.github.streamingwithflink.chapter8.drds;

import io.github.streamingwithflink.chapter8.CommonPOJO;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class SinkMysqlFunction<IN extends CommonPOJO> extends RichSinkFunction<IN> {
    private MysqlClient dbclient;

    @Override
    public void open(Configuration parameters) throws Exception {
        dbclient = new MysqlClient();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        dbclient.writeDB(value);
    }
}
