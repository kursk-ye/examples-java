package io.github.streamingwithflink.chapter8.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;

public class OdpsClient {
  private Odps odps = null;
  private final String ACCESS_ID = "**";
  private final String ACCESS_KEY = "**";
  private final String ENDPOINT = "http://service.cn-shanghai.maxcompute.aliyun.com/api";
  private final String PROJECT_NAME = "**";

  public OdpsClient() {
    Account account = new AliyunAccount(ACCESS_ID, ACCESS_KEY);
    this.odps = new Odps(account);
    odps.setEndpoint(ENDPOINT);
    odps.setDefaultProject(PROJECT_NAME);
  }

  public Odps getOdps() {
    return this.odps;
  }
}
