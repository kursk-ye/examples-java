package io.github.streamingwithflink.chapter8.datahub.tuple;

import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.datahub.clientlibrary.consumer.Consumer;
import io.github.streamingwithflink.chapter8.PoJoElecMeterReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataHubTupleSource extends DataHubSource<PoJoElecMeterReading> {
  private Boolean running = true;
  private static final Logger LOG = LoggerFactory.getLogger(DatahubTupleConsumer.class);

  @Override
  public void run(SourceContext<PoJoElecMeterReading> ctx) throws Exception {
    String endpoint = "https://dh-cn-shanghai.aliyuncs.com";
    String accessId = "***";
    String accessKey = "***";
    String project = "yecustomproject3";
    String topic = "kmg_tuple";
    String subId = "1594612865441CSK4M";

    Consumer dataConsumer =
        DatahubTupleConsumer.createConsumer(endpoint, accessId, accessKey, project, topic, subId);

    int maxRetry = 3;

    try {
      while (running) {
        try {
          while (true) {
            // 协同消费刚初始化，需要等待服务端分配shard，约40秒，期间只能返回null
            // 自动提交模式，每次调用read，认为之前读的数据都已处理完成，自动ack
            RecordEntry record = dataConsumer.read(maxRetry);
            // 处理数据
            if (record != null) {
              TupleRecordData data = (TupleRecordData) record.getRecordData();
              // 根据自己的schema来处理数据，此处打印第一列的内容
              // LOG.info("id: {}", data.getField("id"));
              ctx.collect(new PoJoElecMeterReading(){{
                this.setId((String)data.getField("id"));
                this.setTimestamp((Long)data.getField("timestamp"));
                this.setDayelecvalue((Double) data.getField("dayelecvalue"));
              }});

              // 根据列名取数据
              // LOG.info("field2: {}", data.getField("field2"));
              // 非自动提交模式，每条record处理完后都需要ack
              // 自动提交模式，ack不会做任何操作
              // 1.1.7版本及以上
              record.getKey().ack();
            } else {
              LOG.info("read null");
            }
          }

        } catch (SubscriptionOffsetResetException e) {

          // 点位被重置，重新初始化consumer
          try {
            dataConsumer.close();
            dataConsumer =
                DatahubTupleConsumer.createConsumer(
                    endpoint, accessId, accessKey, project, topic, subId);
          } catch (DatahubClientException e1) {
            // 初始化失败，重试或直接抛异常
            LOG.error("create consumer failed", e);
            throw e;
          }

        } catch (InvalidParameterException
            | SubscriptionOfflineException
            | SubscriptionSessionInvalidException
            | AuthorizationFailureException
            | NoPermissionException e) {
          // 请求参数非法
          // 订阅被下线
          // 订阅下相同shard被其他客户端占用
          // 签名不正确
          // 没有权限
          LOG.error("read failed", e);
          throw e;
        } catch (DatahubClientException e) {
          // 基类异常，包含网络问题等，可以选择重试
          LOG.error("read failed, retry", e);
          DatahubTupleConsumer.sleep(1000);
        }
      }
    } catch (Throwable e) {
      LOG.error("read failed", e);
    } finally {
      // 确保资源正确释放
      // 会提交已ack的点位
      dataConsumer.close();
    }
  }

  @Override
  public void cancel() {
    this.running = false;
  }
}
