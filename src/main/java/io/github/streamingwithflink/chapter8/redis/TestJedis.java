package io.github.streamingwithflink.chapter8.redis;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class TestJedis {
  public static void main(String[] args) {
    //
    HostAndPort hp = new HostAndPort("hdkj-puda20200518.redis.rds.aliyuncs.com", 6379);
    Jedis jedis = new Jedis(hp);

    String passwd = "2020@Wuhan";
    jedis.auth(passwd);

    jedis.select(1);
    System.out.println(jedis.ping());

    System.out.println(jedis.set("jedis1", "good key"));
    System.out.println(jedis.get("jedis1"));

    ///

    Map<String, Double> scores = new HashMap<>();

    scores.put("PlayerOne", 3000.0);
    scores.put("PlayerTwo", 1500.0);
    scores.put("PlayerThree", 8200.0);

    scores.entrySet().forEach(playerScore -> {
      jedis.zadd("ranking", playerScore.getValue(), playerScore.getKey());
    });

    String player = jedis.zrevrange("ranking", 0, 1).iterator().next();
    System.out.println(player);
    long rank = jedis.zrevrank("ranking", "PlayerOne");
    System.out.println(rank);

    ///
  }
}
