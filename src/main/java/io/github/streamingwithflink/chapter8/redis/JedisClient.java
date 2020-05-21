package io.github.streamingwithflink.chapter8.redis;

import io.github.streamingwithflink.chapter8.CommonPOJO;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Set;

public class JedisClient {
    private final transient HostAndPort hp = new HostAndPort("***", 110);
    private final transient String passwd = "****";
    private Jedis jedis;

    public JedisClient(){
        jedis = new Jedis(hp);
        jedis.auth(passwd);
    }

    public <T extends CommonPOJO> void setRecord(String key,T record){
        HashMap<String,String> hm = new HashMap<>();
        hm.put("value" , record.getValue().toString());
        jedis.hset(key, hm);
    }

    public Set<String> getALLKeys(String pattern){
        return jedis.keys(pattern);
    }

    public String  hget(String key,String value){
        return jedis.hget(key,value);
    }

    public Long del(String key){
        return jedis.del(key);
    }

}
