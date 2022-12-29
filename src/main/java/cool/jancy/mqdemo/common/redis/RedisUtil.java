package cool.jancy.mqdemo.common.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author dengjie
 * @version 1.0
 * @ClassName : RedisUtil
 * @description: TODO
 * @date 2022/03/14 18:24
 */
@SpringBootConfiguration
@EnableCaching
@Transactional
@Slf4j
public class RedisUtil {

    private static RedisTemplate<String, Object> redisTemplate;

    @Autowired
    public RedisUtil(RedisTemplate<String, Object> redisTemplate) {
        RedisUtil.redisTemplate = redisTemplate;
    }


    public static Object get(String key) {
        try {
            Object value = redisTemplate.opsForValue().get(key);
            return value;
        } catch (NullPointerException e) {
            log.error("get error", e);
            return null;
        }
    }

    public static boolean putCacheWithExpireTime(String key, Object value, long time) {
        try {
            if (key != null && value != null && time > 0) {
                redisTemplate.opsForValue().set(key, value, time, TimeUnit.SECONDS);
                return true;
            }
            return false;
        } catch (Exception e) {
            log.error("putCacheWithExpireTime error", e);
            return false;
        }
    }

    public static boolean put(String key, Object value) {
        try {
            redisTemplate.opsForValue().set(key, value);
            return true;
        } catch (Exception e) {
            log.error("put error", e);
            return false;
        }
    }

    public static boolean del(String key) {
        try {
            return redisTemplate.delete(key);
        } catch (Exception e) {
            log.error("del error", e);
            return false;
        }
    }

    public static boolean expire(String key, int seconds) {
        try {
            return redisTemplate.expire(key, seconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("expire error", e);
            return false;
        }
    }

    public static Long incr(String key) {
        Long num = null;
        try {
            num = redisTemplate.opsForValue().increment(key);
            return num;
        } catch (Exception e) {
            log.error("incr error", e);
            return num;
        }
    }

    public static Set<String> keys(String pattern) {
        Set<String> keys = null;
        try {
            keys = redisTemplate.keys(pattern);
            return keys;
        } catch (Exception e) {
            log.error("keys error", e);
            return keys;
        }
    }

    public static Long size(String key) {
        Long num = null;
        try {
            num = redisTemplate.opsForValue().size(key);
            return num;
        } catch (Exception e) {
            log.error("size error", e);
            return num;
        }
    }

    public static Long totalSize() {
        Long num = null;
        try {
            Set<String> keys = redisTemplate.keys("*");
            for (String key : keys) {
                num += redisTemplate.opsForValue().size(key);
            }
            return num;
        } catch (Exception e) {
            log.error("size error", e);
            return num;
        }
    }

    public static boolean flush() {
        try {
            Set<String> keys = redisTemplate.keys("*");
            Long nums = redisTemplate.delete(keys);
            log.info("delete rows :: {}", nums);
            return true;
        } catch (Exception e) {
            log.error("expire error", e);
            return false;
        }
    }
}
