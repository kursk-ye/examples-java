package io.github.streamingwithflink.chapter8.odps;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;

/**
 * 将Datahub中的增量记录(以下简称“增量表”)合并到ODPS的表(以下简称“历史表”)中 为了简化过程，增量表和历史表的表结构一致 增量表和历史表都使用ID作为主键
 * 插入中，如果历史表中存在增量表相关的主键ID，则使用增量表的记录覆盖历史表； 插入中，如果历史表中不存在增量表相关的主键ID，增量表的记录直接插入；
 * INSERT OVERWRITE TABLE
 * kmg_tuple_increa
 * VALUES
 * ('id16','2020','1000.1'),
 * ('id2','2020','1020.2');
 *
 * INSERT OVERWRITE TABLE
 * kmg_tuple
 * VALUES
 * ('id1','2020','10.1'),
 * ('id2','2020','20.2'),
 * ('id3','2020','30.2'),
 * ('id4','2020','40.2');
 *
 */
public class OverwriteODPS {
  private static  String increaseTableName = "";
  private static String historyTableName = "";
  private static Odps odps;

  public static void main(String[] args) throws OdpsException {
    OdpsClient client = new OdpsClient();
    odps = client.getOdps();

    String insertSql =
        "INSERT OVERWRITE table kmg_tuple\n"
            + "SELECT \n"
            + "case when t1id is NOT NULL then t1id else t2id END  as elecid,\n"
            + "case when t1time is NOT NULL then t1time else t2time END  as electimestamp,\n"
            + "case when t1value is NOT NULL then t1value else t2value END  as dayelecvalue\n"
            + "from \n"
            + "(SELECT t1.elecid as t1id ,\n"
            + "        t1.electimestamp as t1time,\n"
            + "        t1.dayelecvalue as t1value ,\n"
            + "        t2.elecid as t2id,\n"
            + "        t2.electimestamp as t2time,\n"
            + "        t2.dayelecvalue as t2value\n"
            + "from kmg_tuple_increa  t1\n"
            + "full outer join kmg_tuple t2\n"
            + "on t1.elecid = t2.elecid);";

    Instance i = SQLTask.run(odps,insertSql);
    i.waitForSuccess();

  }

}
