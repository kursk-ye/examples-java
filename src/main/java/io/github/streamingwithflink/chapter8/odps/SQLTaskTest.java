package io.github.streamingwithflink.chapter8.odps;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;

import java.util.List;

public class SQLTaskTest {
  private static final String TableName ="kmg_tuple";
  private static Odps odps;

  public static void main(String[] args) {
    //
    OdpsClient client = new OdpsClient();
    odps = client.getOdps();

    System.out.println(TableName);
    runSql();
    // tunnel();
  }

  /*
   * 运行SQL ,把查询结果保存成临时表，方便后续用Tunnel下载。
   * 保存数据的lifecycle此处设置为1天，如果删除步骤失败，也不会浪费过多存储空间。
   * */
  private static void runSql() {
    Instance i;
    StringBuilder strBuilder =
        new StringBuilder("select elecid, electimestamp, dayelecvalue from ").append(TableName).append(" where elecid='id11';");
    try {
      System.out.println(strBuilder.toString());
      i = SQLTask.run(odps, strBuilder.toString());
      i.waitForSuccess();
      List<Record> records = SQLTask.getResult(i);
      for(Record r:records){
        System.out.println(r.get("dayelecvalue").toString());
      }
    } catch ( OdpsException e) {
      e.printStackTrace();
    }
  }
}
