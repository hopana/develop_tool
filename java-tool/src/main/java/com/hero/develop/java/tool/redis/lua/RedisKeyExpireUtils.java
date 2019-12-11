package com.hero.develop.java.tool.redis.lua;

import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.regex.Pattern;

/**
 * 对redis的key做超时操作的工具类
 * @Author zhanghao
 * @date 2019/4/11 14:34
 **/
@Component
public class RedisKeyExpireUtils {

    private static final String pattern = "^\\{.+?\\}$";

    private static StringRedisTemplate stringRedisTemplate;


    private static RedisTemplate<String,Object> redisTemplate;

    @Autowired
    public  void setStringRedisTemplate(StringRedisTemplate stringRedisTemplate) {
        RedisKeyExpireUtils.stringRedisTemplate = stringRedisTemplate;
    }

    @Autowired
    public  void setRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
        RedisKeyExpireUtils.redisTemplate = redisTemplate;
    }

    /**
     * 对一个键进行自增1，并设置超时
     * @Author zhanghao
     * @Date  2019/4/11
     * @Param [key, delta, expireTime]
     * @return java.lang.Long
     **/
    public static Long incrAndSetExpireIfAbsent( String key, Duration expireTime){
        return incrAndSetExpireIfAbsent(key,1,expireTime);
    }

    /**
     * 对一个键进行自增1，并设置超时
     * 如该Key已有超时时间，则会刷新
     * @Author zhanghao
     * @Date  2019/4/11
     * @Param [key, delta, expireTime]
     * @return java.lang.Long
     **/
    public static Long incrAndSetExpire( String key, Duration expireTime){
        return incrAndSetExpire(key,1,expireTime);
    }

    /**
     * 对一个键进行自增指定的长度，并在该key没有超时时间时设置超时
     * @Author zhanghao
     * @Date  2019/4/11
     * @Param [key, delta, expireTime]
     * @return java.lang.Long
     **/
    public static Long incrAndSetExpireIfAbsent( String key, Integer delta, Duration expireTime){
        final long expireMillis = expireTime.toMillis();
        return stringRedisTemplate.execute(LuaRedisScriptConfig.INCR_AND_SET_EXPIRE_IF_ABSENT_SCRIPT, Lists.newArrayList(key), String.valueOf(delta) ,String.valueOf(expireMillis));
    }

    /**
     * 对一个键进行自增指定的长度，并设置超时
     * 如该Key已有超时时间，则会刷新
     * @Author zhanghao
     * @Date  2019/4/11
     * @Param [key, delta, expireTime]
     * @return java.lang.Long
     **/
    public static Long incrAndSetExpire( String key, Integer delta, Duration expireTime){
        final long expireMillis = expireTime.toMillis();
        return stringRedisTemplate.execute(LuaRedisScriptConfig.INCR_AND_SET_EXPIRE_SCRIPT ,Lists.newArrayList(key), String.valueOf(delta) ,String.valueOf(expireMillis));
    }


    /**
     * 将值推入列表的左边，并设置过期时间
     * * 如该Key已有超时时间，则会刷新
     * @param key
     * @param expireTime
     * @param values
     * @return
     */
    public static Long listPushLeftAndSetExpire(String key , Duration expireTime ,Object... values  ){
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.LIST_PUSH_LEFT_AND_SET_EXPIRE_SCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis))), values);
    }

    /**
     * 将值推入列表的左边，并在该key没有超时时间时设置超时
     * @param key
     * @param expireTime
     * @param values
     * @return
     */
    public static Long listPushLeftAndSetExpireIfAbsent(String key , Duration expireTime ,Object... values  ){
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.LIST_PUSH_LEFT_AND_SET_EXPIRE_IF_ABSENT_SCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis))), values);
    }

    /**
     * 将值推入列表的右边，并设置过期时间
     * *如该Key已有超时时间，则会刷新
     * @param key
     * @param expireTime
     * @param values
     * @return
     */
    public static Long listPushRightAndSetExpire(String key , Duration expireTime ,Object... values  ){
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.LIST_PUSH_RIGHT_AND_SET_EXPIRE_SCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis))), values);
    }

    /**
     * 将值推入列表的右边，并在该key没有超时时间时设置超时
     * @param key
     * @param expireTime
     * @param values
     * @return
     */
    public static Long listPushRightAndSetExpireIfAbsent(String key , Duration expireTime ,Object... values  ){
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.LIST_PUSH_RIGHT_AND_SET_EXPIRE_IF_ABSENT_SCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis))), values);
    }


    /**
     * 将值加入到set集合中，并设置过期时间
     * *如该Key已有超时时间，则会刷新
     * @param key
     * @param expireTime
     * @param values
     * @return
     */
    public static Long setAddAndSetExpire(String key , Duration expireTime ,Object... values  ){
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.SET_ADD_AND_SET_EXPIRE_SSCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis))), values);
    }
    /**
     * 将值加入到set集合中，并在该key没有超时时间时设置超时
     * @param key
     * @param expireTime
     * @param values
     * @return
     */
    public static Long setAddAndSetExpireIfAbsent(String key , Duration expireTime ,Object... values  ){
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.SET_ADD_AND_SET_EXPIRE_IF_ABSENT_SCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis))), values);
    }


    /**
     * 将值加入到Map中，并设置过期时间
     * 如该Key已有超时时间，则会刷新
     * @param key
     * @param expireTime
     * @param filedKey
     * @param value
     * @return
     */
    public static Long mapPutAndSetExpire(String key , Duration expireTime ,String filedKey ,Object value ){
        Assert.isTrue(Pattern.matches(pattern,key),"key 必须包括在{}中");
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.MAP_PUT_AND_SET_EXPIRE_SCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis)).concat(",").concat(filedKey)), value);
    }
    /**
     * 将值加入到Map中，并在该key没有超时时间时设置超时
     * @param key
     * @param expireTime
     * @param filedKey
     * @param value
     * @return
     */
    public static Long mapPutAndSetExpireIfAbsent(String key , Duration expireTime ,String filedKey ,Object value ){
        Assert.isTrue(Pattern.matches(pattern,key),"key 必须包括在{}中");
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.MAP_PUT_AND_SET_EXPIRE_IF_ABSENT_SCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis)).concat(",").concat(filedKey)), value);
    }


    /**
     * 将值加入到有序列表中，并设置过期时间
     * 如该Key已有超时时间，则会刷新
     * @param key
     * @param expireTime
     * @param score
     * @param value
     * @return
     */
    public static Boolean zsetAddAndSetExpire(String key , Duration expireTime ,Double score ,Object value ){
        Assert.isTrue(Pattern.matches(pattern,key),"key 必须包括在{}中");
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.ZSET_ADD_AND_SET_EXPIRE_SCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis)).concat(",").concat(String.valueOf(score))), value);
    }
    /**
     * 将值加入到有序列表中，并在该key没有超时时间时设置超时
     * @param key
     * @param expireTime
     * @param score
     * @param value
     * @return
     */
    public static Boolean zsetAddAndSetExpireIfAbsent(String key , Duration expireTime ,Double score ,Object value ){
        Assert.isTrue(Pattern.matches(pattern,key),"key 必须包括在{}中");
        final long expireMillis = expireTime.toMillis();
        return redisTemplate.execute(LuaRedisScriptConfig.ZSET_ADD_AND_SET_EXPIRE_IF_ABSENT_SCRIPT,
                Lists.newArrayList(key.concat(",").concat(String.valueOf(expireMillis)).concat(",").concat(String.valueOf(score))), value);
    }


}
