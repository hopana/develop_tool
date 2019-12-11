package com.hero.develop.java.tool.redis.batch;

import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * reids 批量查询工具类
 * @Author zhanghao
 * @date 2019/4/17 10:50
 **/
@Component
public class RedisBatchQueryUtils {


    private static RedisTemplate<String, Object> redisTemplate;

    @Autowired
    public  void setRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
        RedisBatchQueryUtils.redisTemplate = redisTemplate;
    }


    /**
     * @Author zhanghao
     * @Description 根据 keys 批量获取String 类型的值
     * @Date  2019/4/17
     * @Param [keys]
     * @return java.util.List<T>
     **/
    public static  <T> List<T> getValues(List<String> keys){
        return (List<T>) redisTemplate.executePipelined((RedisCallback<T>) connection -> {
            final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
            keys.forEach(e -> {
                connection.get(keySerializer.serialize(e));
            });
            return null;
        }).stream().filter(Objects::nonNull).collect(Collectors.toList());
    }




    /**
     * @Author zhanghao
     * @Description 批量获取Hash的值
     * @Date  2019/4/17
     * @Param [hashKeys] 值分别为 redisKey ,fieldKey
     * @return java.util.List<T>
     **/
    public static <T> List<T> getHashValues(List<Tuple2<String, String>> hashKeys){
        return (List<T>) redisTemplate.executePipelined((RedisCallback<T>) connection -> {
            final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
            final RedisSerializer hashKeySerializer = redisTemplate.getHashKeySerializer();
            final RedisHashCommands redisHashCommands = connection.hashCommands();
            hashKeys.forEach(e -> {
                redisHashCommands.hGet(keySerializer.serialize(e.getT1()), hashKeySerializer.serialize(e.getT2()));
            });
            return null;
        }).stream().filter(Objects::nonNull).collect(Collectors.toList());
    }



    /**
     * @Author zhanghao
     * @Description 批量获取Hash的值
     * @Date  2019/4/17
     * @Param [hashKeys] 值分别为 redisKey ,fieldKey
     * @return java.util.List<T>
     **/
    public static <T> Map<Tuple2<String, String>,T> getHashValueMap(List<Tuple2<String, String>> hashKeys){
        Map<Integer, Tuple2<String, String>> resultIndex = Maps.newHashMap();
        AtomicInteger atomicInteger = new AtomicInteger();
        final List<T> resultList = (List<T>) redisTemplate.executePipelined((RedisCallback<T>) connection -> {
            final RedisSerializer keySerializer = redisTemplate.getKeySerializer();
            final RedisSerializer hashKeySerializer = redisTemplate.getHashKeySerializer();
            final RedisHashCommands redisHashCommands = connection.hashCommands();
            hashKeys.forEach(e -> {
                redisHashCommands.hGet(keySerializer.serialize(e.getT1()), hashKeySerializer.serialize(e.getT2()));
                resultIndex.put(atomicInteger.getAndIncrement(),e);
            });
            return null;
        }).stream().collect(Collectors.toList());
        return IntStream.range(0, resultList.size())
                .mapToObj(i ->  Objects.nonNull(resultList.get(i)) ? null : Tuples.of(resultIndex.get(i), resultList.get(i)))
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(t -> t.getT1(), t -> t.getT2()));
    }






}
