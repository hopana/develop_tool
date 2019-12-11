package com.hero.develop.java.tool.redis.batch;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * redis 批量操作数据工具类
 * 用于批量操作不同的 redis key
 * @Author zhanghao
 * @date 2019/4/17 14:18
 **/
@Component
public class RedisBatchOperationUtils {


    private static RedisTemplate<String, Object> redisTemplate;

    @Autowired
    public void setRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
        RedisBatchOperationUtils.redisTemplate = redisTemplate;
    }


    /**
     * @return void
     * @Author zhanghao
     * @Description 批量添加 String 值
     * @Date 2019/4/17
     * @Param [values] 添加的值Map key 为 Redis key , value 为要添加的值
     **/
    public static void addValues(Map<String, Object> values) {
        if (MapUtils.isNotEmpty(values)){
            redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
                    final RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
                    final RedisStringCommands stringCommands = connection.stringCommands();
                    values.forEach((k, v) -> {
                        stringCommands.set(keySerializer.serialize(k),valueSerializer.serialize(v));
                    });
                    return null;
                }
            });
        }
    }

    /**
     * @Author zhanghao
     * @Description 批量添加 String 值
     * @Date  2019/4/17
     * @Param [values] 添加的值列表，三个值分别为 redisKey , value , 过期时间
     * @return void
     **/
    public static void addValues(List<Tuple3<String, Object, Duration>> values){
        if (CollectionUtils.isNotEmpty(values)){
            redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
                    final RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
                    values.forEach(value -> {
                        connection.pSetEx(keySerializer.serialize(value.getT1()),value.getT3().toMillis(),valueSerializer.serialize(value.getT2()));
                    });
                    return null;
                }
            });
        }
    }



    /**
     * @Author zhanghao
     * @Description 批量从左边推入 List 值
     * @Date  2019/4/17
     * @Param [values] 添加的值列表，两个值分别为 redisKey , value列表
     * @return void
     **/
    public static void leftPushListValues(List<Tuple2<String, List<Object>>> values){
        if (CollectionUtils.isNotEmpty(values)){
            redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
                    final RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
                    final RedisListCommands redisListCommands = connection.listCommands();
                    values.forEach(t -> {
                        final List<Object> list = t.getT2();
                        byte[][] bytes = new byte[list.size()][];
                        bytes = list.stream().map(e -> valueSerializer.serialize(e)).collect(Collectors.toList()).toArray(bytes);
                        redisListCommands.lPush(keySerializer.serialize(t.getT1()),bytes);
                    });
                    return null;
                }
            });
        }
    }


    /**
     * @Author zhanghao
     * @Description 批量从右边推入 List 值
     * @Date  2019/4/17
     * @Param [values] 添加的值列表，两个值分别为 redisKey , value列表
     * @return void
     **/
    public static void rightPushListValues(List<Tuple2<String, List<Object>>> values){
        if (CollectionUtils.isNotEmpty(values)){
            redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
                    final RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
                    final RedisListCommands redisListCommands = connection.listCommands();
                    values.forEach(t -> {
                        final List<Object> list = t.getT2();
                        byte[][] bytes = new byte[list.size()][];
                        bytes = list.stream().map(e -> valueSerializer.serialize(e)).collect(Collectors.toList()).toArray(bytes);
                        redisListCommands.rPush(keySerializer.serialize(t.getT1()),bytes);
                    });
                    return null;
                }
            });
        }
    }


    /**
     * @Author zhanghao
     * @Description 批量添加set值
     * @Date  2019/4/17
     * @Param [values] 添加的值列表，两个值分别为 redisKey , value列表
     * @return void
     **/
    public static void addSetValues(List<Tuple2<String, List<Object>>> values){
        if (CollectionUtils.isNotEmpty(values)){
            redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
                    final RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
                    final RedisSetCommands redisSetCommands = connection.setCommands();
                    values.forEach(t -> {
                        final List<Object> list = t.getT2();
                        byte[][] bytes = new byte[list.size()][];
                        bytes = list.stream().map(e -> valueSerializer.serialize(e)).collect(Collectors.toList()).toArray(bytes);
                        redisSetCommands.sAdd(keySerializer.serialize(t.getT1()),bytes);
                    });
                    return null;
                }
            });
        }
    }



    /**
     * @Author zhanghao
     * @Description 批量添加hash值
     * @Date  2019/4/17
     * @Param [values] 添加的值列表分别为 redisKey , fieldKey , value
     * @return void
     **/
    public static void putHashValues(List<Tuple3<String, String, Object>> values){
        if (CollectionUtils.isNotEmpty(values)){
            redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
                final RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
                final RedisSerializer hashKeySerializer = redisTemplate.getHashKeySerializer();
                final RedisHashCommands redisHashCommands = connection.hashCommands();
                values.forEach(t -> {
                    redisHashCommands.hSet(keySerializer.serialize(t.getT1()),hashKeySerializer.serialize(t.getT2()),valueSerializer.serialize(t.getT3()));
                });
                return null;
            });
        }
    }


    /**
     * @author zhanghao
     * @description  批量删除Hash的值
     * @date  2019/6/12
     * @param keyAndFieldKeyMap key - fieldKey
     * @return void
     **/
    public static void deleteHashValues(Map<String, String> keyAndFieldKeyMap){
        if (MapUtils.isNotEmpty(keyAndFieldKeyMap)){
            redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
                final RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
                final RedisSerializer hashKeySerializer = redisTemplate.getHashKeySerializer();
                final RedisHashCommands redisHashCommands = connection.hashCommands();
                keyAndFieldKeyMap.forEach((key,fieldKey) -> {
                    redisHashCommands.hDel(keySerializer.serialize(key),hashKeySerializer.serialize(fieldKey));
                });
                return null;
            });
        }

    }



    /**
     * @Author zhanghao
     * @Description 批量添加zset值
     * @Date  2019/4/17
     * @Param [values] 添加的值列表分别为 redisKey ,分值 , value列表
     * @return void
     **/
    public static void addZSetValues(List<Tuple3<String, Double, Object>> values){
        if(CollectionUtils.isNotEmpty(values)){
            redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
                    final RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
                    final RedisZSetCommands redisZSetCommands = connection.zSetCommands();
                    values.forEach(t -> {
                        redisZSetCommands.zAdd(keySerializer.serialize(t.getT1()),t.getT2(),valueSerializer.serialize(t.getT3()));
                    });
                    return null;
                }
            });
        }

    }




}
