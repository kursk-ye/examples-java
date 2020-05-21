package io.github.streamingwithflink.chapter8.redis;

import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Set;

public class RedisSourceFunction extends RichSourceFunction<PoJoElecMeterReading> {
  private Boolean running = true;
  private JedisClient jedis;
  private Set<String> sourceSet;

  @Override
  public void open(Configuration parameters) throws Exception {
    jedis = new JedisClient();
    sourceSet = jedis.getALLKeys("*MID*");
    if (sourceSet.isEmpty()){
        this.cancel();
    }
  }

  @Override
  public void run(SourceContext<PoJoElecMeterReading> ctx) throws Exception {
    while (running) {
      for (String key : sourceSet) {
        String strKey = key.substring(key.indexOf("-") + 1, key.length());
        Long ts = System.currentTimeMillis();
        Double value = Double.parseDouble(jedis.hget(key,"value")) ;
        PoJoElecMeterReading record = new PoJoElecMeterReading();
        record.setId(strKey);
        record.setTimestamp(ts);
        record.setDayelecvalue(value);

        ctx.collect(record);
        jedis.del(key);
      }
    }
  }

  @Override
  public void cancel() {
    this.running = false;
  }
}
