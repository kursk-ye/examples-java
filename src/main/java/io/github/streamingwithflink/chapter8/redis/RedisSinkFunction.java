package io.github.streamingwithflink.chapter8.redis;

import io.github.streamingwithflink.chapter8.CommonPOJO;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class RedisSinkFunction<IN extends CommonPOJO> extends RichSinkFunction<IN> {
    private JedisClient jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new JedisClient();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        String key = System.currentTimeMillis() + "-" +value.getKey();
        jedis.setRecord(key,value);
    }
}
