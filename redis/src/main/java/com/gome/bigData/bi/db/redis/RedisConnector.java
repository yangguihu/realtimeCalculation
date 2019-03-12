package com.gome.bigData.bi.db.redis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 获取到指定redis的连接
 * 提供对redis的hash数据结构的增删改查功能
 * 执行完增删改查之后,自动返回连接到JedisPool中
 * Created by MaLi on 2017/1/9.
 */
public class RedisConnector implements Serializable{
    //测试时候指定的redis的库
    private static final int dbnum=7;
    private static JedisPool pool = null;

    public RedisConnector(String host, int port) {
        connect(host, port);
    }

    public RedisConnector() {
    }

    /**
     * 获取到原生Jedis,供使用原生API的类去调用
     * @return
     */
    public Jedis getJedis(){
        return pool.getResource();
    }

    /**
     * 释放原生Jedis,供使用原生API的类去调用
     * @param jedis
     */
    public void releaseJedis(Jedis jedis){
        pool.returnResourceObject(jedis);
    }


    /**
     * 在连接池中获取连接
     *
     * @param host
     * @param port
     */
    public void connect(String host, int port) {
        if (pool == null) {
            synchronized (RedisConnector.class) {
                if (pool == null) {
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxTotal(-1);
                    poolConfig.setMaxIdle(10000);
                    poolConfig.setMaxWaitMillis(100000);
                    poolConfig.setTestOnBorrow(true);
                    //测试使用6号库
                    pool = new JedisPool(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT,null, RedisConnector.dbnum);

                    //public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
                    //int timeout, final String password, final int database) {
                }
            }
        }
    }

    /**
     * 是否存在这个key
     * @param key
     * @return
     */
    public boolean existsKey(String key){
        boolean flag = false;
        Jedis jedis = pool.getResource();
        if(jedis.exists(key)){
            flag = true;
        }
        pool.returnResourceObject(jedis);
        return flag;
    }



    /**
     * 删除0号数据库中的某个key
     *
     * @param key Redis的key
     */
    public void deleteKey(String key) {
        deleteKey(RedisConnector.dbnum, key);
    }

    /**
     * 删除0号数据库中的多个key
     *
     * @param keys Redis的key
     */
    public void deleteKeys(List<String> keys) {
        deleteKeys(RedisConnector.dbnum, keys);
    }

    /**
     * 删除指定数据库中的某个key
     *
     * @param dbName 指定的数据库
     * @param key    Redis的key
     */
    public void deleteKey(int dbName, String key) {

        Jedis jedis = pool.getResource();
        jedis.del(key);
        pool.returnResourceObject(jedis);
    }

    /**
     * 删除指定数据库中的多个key
     *
     * @param dbName 指定的数据库
     * @param keys   Redis的key
     */
    public void deleteKeys(int dbName, List<String> keys) {
        Jedis jedis = pool.getResource();
        for (String key : keys) {
            jedis.del(key);
        }
        pool.returnResourceObject(jedis);
    }

    /**
     * 保存String类型值到set数据结构,默认使用0号数据库
     * @param key Redis的key
     * @param value  Set数据结构中的key
     */
    public void save2set(String key,String value){
        save2set(RedisConnector.dbnum,key,value);
    }
    /**
     * 保存String类型值到set数据结构
     * @param dbName 指定的数据库
     * @param key Redis的key
     * @param value  Set数据结构中的key
     */
    public void save2set(int dbName,String key,String value){
        Jedis jedis = pool.getResource();
        jedis.select(dbName);
        jedis.sadd(key,value);
        pool.returnResourceObject(jedis);
    }

    /**
     * 保存字段和字段值到hash数据结构,默认选择0号数据库.
     *
     * @param key    Redis的key
     * @param values Map结构里面存储的字段和字段值
     */
    public void save2hash(String key, Map<String, Object> values) {
        save2hash(RedisConnector.dbnum, key, values);
    }

    /**
     * 保存字段和字段值到hash数据结构
     *
     * @param dbName 指定的数据库
     * @param key    Redis的key
     * @param values Map结构里面存储的字段和字段值
     */
    public void save2hash(int dbName, String key, Map<String, Object> values) {
        Jedis jedis = pool.getResource();
        jedis.select(dbName);
        for (Map.Entry<String, Object> e : values.entrySet()) {
            String value = (String) e.getValue();
            if(value!=null){
                jedis.hset(key, e.getKey(), value);
            }
        }
        pool.returnResourceObject(jedis);
    }
    /**
     * 获取Hash数据结构里面的指定key的值,默认使用0号数据库
     *
     * @param key   Redis的key
     * @param field hash数据结构中的字段
     * @return
     */
    public String getFromHash(String key, String field) {
        return getFromHash(RedisConnector.dbnum, key, field);
    }


    /**
     * 判断hash中是否存在指定的字段,默认筛选0#数据库
     * @param keyName
     * @param fieldName
     * @return
     */
    public Boolean existsInHash(String keyName,String fieldName){
        return existsInHash(RedisConnector.dbnum,keyName,fieldName);
    }

    /**
     * 判断hash中是否存在指定的字段
     * @param dbName
     * @param keyName
     * @param fieldName
     * @return
     */
    public Boolean existsInHash(int dbName,String keyName,String fieldName){
        Jedis jedis = pool.getResource();
        jedis.select(dbName);
        Boolean hexists = jedis.hexists(keyName, fieldName);
        pool.returnResourceObject(jedis);

        return hexists;
    }




    /**
     * 判断指定的member值是否存在于set数据结构的0号数据库中里面
     * @param keyName Redis的key
     * @param member 指定的member
     * @return
     */
    public Boolean existsInSet(String keyName,String member){
        return existsInSet(RedisConnector.dbnum,keyName,member);
    }

    /**
     * 判断指定的member值是否存在于set数据结构里面
     * @param dbName 指定数据库
     * @param keyName Redis的key
     * @param member 指定的member
     * @return
     */
    public Boolean existsInSet(int dbName,String keyName,String member){
        Jedis jedis = pool.getResource();
        jedis.select(dbName);
        Boolean sismember = jedis.sismember(keyName, member);
        pool.returnResourceObject(jedis);
        return sismember;
    }


    /**
     * 获取Hash数据结构里面的指定key的值
     *
     * @param dbName 指定的数据库
     * @param key    Redis的key
     * @param field  hash数据结构中的字段
     * @return
     */
    public String getFromHash(int dbName, String key, String field) {
        Jedis jedis = pool.getResource();
        jedis.select(dbName);
        String value = jedis.hget(key, field);
        pool.returnResourceObject(jedis);
        return value;
    }

    /**
     * 给hash结构中某个key的指定字段自增"1",默认使用0号数据库
     *
     * @param key      Redis的key
     * @param keyField hash结构中的字段
     */
    public void addOnece2hash(String key, String keyField) {
        addOnece2hash(RedisConnector.dbnum, key, keyField);
    }

    /**
     * 给hash结构中某个key的指定字段自增"1"
     *
     * @param dbName   指定的数据库
     * @param key      Redis的key
     * @param keyField hash结构中的字段
     */
    public void addOnece2hash(int dbName, String key, String keyField) {
        Jedis jedis = pool.getResource();
        jedis.select(dbName);
        jedis.hincrBy(key, keyField, 1);
        pool.returnResourceObject(jedis);
    }

    /**
     * 修改Hash数据结构中的某个字段的值,默认使用0号数据库
     *
     * @param key    Redis的key
     * @param values 待修改的字段及新的字段值
     */
    public void update2hash(String key, Map<String, Object> values) {
        save2hash(RedisConnector.dbnum, key, values);
    }

    /**
     * 修改Hash数据结构中的某个字段的值
     *
     * @param dbName 指定的数据库
     * @param key    Redis的key
     * @param values 待修改的字段及新的字段值
     */
    public void update2hash(int dbName, String key, Map<String, Object> values) {
        save2hash(dbName, key, values);
    }

    /**
     * 删除hash数据结构中的某个字段的值,默认使用0号数据库
     *
     * @param key    Redis的key
     * @param fields 待删除的字段
     */
    public void deleteFromHash(String key, List<String> fields) {
        deleteFromHash(RedisConnector.dbnum, key, fields);
    }

    /**
     * 删除hash数据结构中的某个字段的值
     *
     * @param dbName 指定的数据库
     * @param key    Redis的key
     * @param fields 待删除的字段
     */
    public void deleteFromHash(int dbName, String key, List<String> fields) {
        Jedis jedis = pool.getResource();
        jedis.select(dbName);
        for (String field : fields) {
            jedis.hdel(key, field);
        }
        pool.returnResourceObject(jedis);
    }
}
