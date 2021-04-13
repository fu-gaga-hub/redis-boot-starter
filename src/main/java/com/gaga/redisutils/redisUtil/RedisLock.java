package com.gaga.redisutils.redisUtil;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Objects;

/**
 * Redis分布式锁类
 * @Author fuGaga
 * @Date 2021/4/13 15:31
 * @Version 1.0
 */
public class RedisLock {

    @Autowired
    private RedisTemplate redisTemplate;

    /**锁键 */
    private static final String lockKey = "redis_lock";
    /**锁时长(ms) */
    private static final long lockExpireMils = 300;

    /**
     * 获取锁
     * @Author fuGaga
     * @Date 2021/4/13 16:33
     * @return boolean
     **/
    public boolean lock(){
        return (Boolean)redisTemplate.execute(new RedisCallback() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                long nowTime = System.currentTimeMillis();
                //setNX获取锁
                boolean acquire = connection.setNX(lockKey.getBytes(), String.valueOf(nowTime + lockExpireMils + 1).getBytes());
                if(acquire){
                    return Boolean.TRUE;
                }else{
                    //获取失败，查询已存在的锁key是否过期
                    byte[] value = connection.get(lockKey.getBytes());
                    if(Objects.nonNull(value) && value.length > 0){
                        long oldTime = Long.parseLong(String.valueOf(value));
                        if(oldTime < nowTime){
                            //已过期，返回这个旧值，并重新设置新值，防止死锁
                            byte[] oldValue = connection.getSet(lockKey.getBytes(), String.valueOf(nowTime + lockExpireMils + 1).getBytes());
                            return null == oldValue ? false : Long.parseLong(String.valueOf(oldValue)) < nowTime;
                        }
                    }
                }
                return false;
            }
        });
    }

    /**
     * 删除锁
     * @Author fuGaga
     * @Date 2021/4/13 16:33
     * @return void
     **/
    public void delLock(){
        redisTemplate.delete(lockKey);
    }
}
