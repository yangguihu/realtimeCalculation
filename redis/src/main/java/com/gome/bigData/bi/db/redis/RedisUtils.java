package com.gome.bigData.bi.db.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by MaLi on 2017/1/6.
 */
public class RedisUtils {
    private static JedisPool pool = null;
    public static synchronized Jedis getJedis(){
        if(pool==null){
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(100);
            poolConfig.setMaxIdle(10);
            poolConfig.setMaxWaitMillis(1000);
            poolConfig.setTestOnBorrow(true);
            pool = new JedisPool(poolConfig,"10.143.90.106",6379);
        }
        Jedis jedis = pool.getResource();
        return jedis;
    }
    public static void release(Jedis jedis){
        pool.returnResourceObject(jedis);
    }
}
