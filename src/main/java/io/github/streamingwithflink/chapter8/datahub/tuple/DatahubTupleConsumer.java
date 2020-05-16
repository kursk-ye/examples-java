package io.github.streamingwithflink.chapter8.datahub.tuple;

import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.consumer.Consumer;

import java.util.concurrent.TimeUnit;

public class DatahubTupleConsumer {

  public static void sleep(long milliSeconds) {
    try {
      TimeUnit.MILLISECONDS.sleep(milliSeconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static Consumer createConsumer(
      String endpoint,
      String accessId,
      String accessKey,
      String project,
      String topic,
      String subId) {
    ConsumerConfig config = new ConsumerConfig(endpoint, accessId, accessKey);
    return new Consumer(project, topic, subId, config);
  }
}
