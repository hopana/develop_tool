package com.hero.develop.java.tool.redis.lua;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

/**
 * lua 脚本配置类
 * @Author zhanghao
 * @date 2019/4/11 14:26
 **/
public class LuaRedisScriptConfig {

    public static final  RedisScript<Long> INCR_AND_SET_EXPIRE_IF_ABSENT_SCRIPT = incrAndSetExpireIfAbsent();

    public static final  RedisScript<Long> INCR_AND_SET_EXPIRE_SCRIPT = incrAndSetExpire();

    public static final  RedisScript<Long> LIST_PUSH_LEFT_AND_SET_EXPIRE_SCRIPT = listPushLeftAndSetExpire();

    public static final  RedisScript<Long> LIST_PUSH_LEFT_AND_SET_EXPIRE_IF_ABSENT_SCRIPT = listPushLeftAndSetExpireIfAbsent();

    public static final  RedisScript<Long> LIST_PUSH_RIGHT_AND_SET_EXPIRE_SCRIPT = listPushRightAndSetExpire();

    public static final  RedisScript<Long> LIST_PUSH_RIGHT_AND_SET_EXPIRE_IF_ABSENT_SCRIPT = listPushRightAndSetExpireIfAbsent();

    public static final  RedisScript<Long> SET_ADD_AND_SET_EXPIRE_SSCRIPT = setAddAndSetExpire();

    public static final  RedisScript<Long> SET_ADD_AND_SET_EXPIRE_IF_ABSENT_SCRIPT = setAddAndSetExpireIfAbsent();

    public static final  RedisScript<Long> MAP_PUT_AND_SET_EXPIRE_SCRIPT = mapPutAndSetExpire();

    public static final  RedisScript<Long> MAP_PUT_AND_SET_EXPIRE_IF_ABSENT_SCRIPT = mapPutAndSetExpireIfAbsent();

    public static final  RedisScript<Boolean> ZSET_ADD_AND_SET_EXPIRE_SCRIPT = zsetAddAndSetExpire();

    public static final  RedisScript<Boolean> ZSET_ADD_AND_SET_EXPIRE_IF_ABSENT_SCRIPT= zsetAddAndSetExpireIfAbsent();


    private static RedisScript<Long> incrAndSetExpireIfAbsent(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/incrAndSetExpireIfAbsent.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }

    private static RedisScript<Long> incrAndSetExpire(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/incrAndSetExpire.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }


    private static RedisScript<Long> listPushLeftAndSetExpire(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/listPushLeftAndSetExpire.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }

    private static RedisScript<Long> listPushLeftAndSetExpireIfAbsent(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/listPushLeftAndSetExpireIfAbsent.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }

    private static RedisScript<Long> listPushRightAndSetExpire(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/listPushRightAndSetExpire.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }

    private static RedisScript<Long> listPushRightAndSetExpireIfAbsent(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/listPushRightAndSetExpireIfAbsent.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }


    private static RedisScript<Long> setAddAndSetExpire(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/setAddAndSetExpire.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }

    private static RedisScript<Long> setAddAndSetExpireIfAbsent(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/setAddAndSetExpireIfAbsent.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }


    private static RedisScript<Long> mapPutAndSetExpire(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/mapPutAndSetExpire.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }

    private static RedisScript<Long> mapPutAndSetExpireIfAbsent(){
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/mapPutAndSetExpireIfAbsent.lua")));
        redisScript.setResultType(Long.class);
        return  redisScript;
    }


    private static RedisScript<Boolean> zsetAddAndSetExpire(){
        DefaultRedisScript<Boolean> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/zsetAddAndSetExpire.lua")));
        redisScript.setResultType(Boolean.class);
        return  redisScript;
    }

    private static RedisScript<Boolean> zsetAddAndSetExpireIfAbsent(){
        DefaultRedisScript<Boolean> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("/luascript/zsetAddAndSetExpireIfAbsent.lua")));
        redisScript.setResultType(Boolean.class);
        return  redisScript;
    }


}
