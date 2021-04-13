package com.gaga.redisutils.redisUtil;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * redis工具类
 *
 * @author fuGaga
 * @date 2020年8月11日
 */
public class RedisTemplateService {

    @Autowired
    RedisTemplate redisTemplate;

    /**
     * key键是否存在
     *
     * @param [key] Redis键
     * @return true=存在;  false=不存在
     * @Author fuGaga
     * @Date 2021/1/28 16:20
     **/
    public boolean hasKey(final String key) {
        return redisTemplate.hasKey(key);
    }

    /**
     * 设置有效时间
     *
     * @param key     Redis键
     * @param timeout 超时时间
     * @return true=设置成功；false=设置失败
     */
    public boolean expire(final String key, final long timeout) {
        return expire(key, timeout, TimeUnit.SECONDS);
    }

    /**
     * 设置有效时间
     *
     * @param key     Redis键
     * @param timeout 超时时间
     * @param unit    时间单位
     * @return true=设置成功；false=设置失败
     */
    public boolean expire(final String key, final long timeout, final TimeUnit unit) {

        Boolean ret = redisTemplate.expire(key, timeout, unit);
        return ret != null && ret;
    }

    /**
     * 删除单个key
     *
     * @param key 键
     * @return true=删除成功；false=删除失败
     */
    public boolean delKey(final String key) {

        Boolean ret = redisTemplate.delete(key);
        return ret != null && ret;
    }

    /**
     * 删除多个key
     *
     * @param keys 键集合
     * @return 成功删除的个数
     */
    public long delKeys(final Collection<String> keys) {

        Long ret = redisTemplate.delete(keys);
        return ret == null ? 0 : ret;
    }

    /**
     * 存入普通对象(key永不过期s)
     *
     * @param key   Redis键
     * @param value 值
     */
    public void setValue(final String key, final Object value) {
        //redisTemplate.opsForValue().set(key, value, 1, TimeUnit.MINUTES);
        redisTemplate.opsForValue().set(key, value);
    }

    // 存储普通对象操作

    /**
     * 存入普通对象
     *
     * @param key     键
     * @param value   值
     * @param timeout 有效期，单位秒
     */
    public void setValueTimeout(final String key, final Object value, final long timeout) {
        redisTemplate.opsForValue().set(key, value, timeout, TimeUnit.SECONDS);
    }

    /**
     * 存入普通对象
     *
     * @param key    键
     * @param value  值
     * @param offset 偏移量
     * @return void
     * @Author fuGaga
     * @Date 2021/1/28 16:33
     **/
    public void setValueRange(final String key, final Object value, final long offset) {
        redisTemplate.opsForValue().set(key, value, offset);
    }

    /**
     * 获取普通对象
     *
     * @param key 键
     * @return 对象
     */
    public Object getValue(final String key) {

        return redisTemplate.opsForValue().get(key);
    }

    // 存储Hash操作

    /**
     * 确定哈希hashKey是否存在
     *
     * @param key  键
     * @param hkey hash键
     * @return true=存在；false=不存在
     */
    public boolean hasHashKey(final String key, String hkey) {

        Boolean ret = redisTemplate.opsForHash().hasKey(key, hkey);
        return ret != null && ret;
    }

    /**
     * 往Hash中存入数据
     *
     * @param key   Redis键
     * @param hKey  Hash键
     * @param value 值
     */
    public void hashPut(final String key, final String hKey, final Object value) {

        redisTemplate.opsForHash().put(key, hKey, value);
    }

    /**
     * 往Hash中存入多个数据
     *
     * @param key    Redis键
     * @param values Hash键值对
     */
    public void hashPutAll(final String key, final Map<String, Object> values) {

        redisTemplate.opsForHash().putAll(key, values);
    }

    /**
     * 获取Hash中的数据
     *
     * @param key  Redis键
     * @param hKey Hash键
     * @return Hash中的对象
     */
    public Object hashGet(final String key, final String hKey) {

        return redisTemplate.opsForHash().get(key, hKey);
    }

    /**
     * 获取Hash中的数据
     *
     * @param key Redis键
     * @return Hash对象
     */
    public Map<Object, Object> hashGetAll(final String key) {
        return redisTemplate.opsForHash().entries(key);
    }

    /**
     * 获取多个Hash中的数据
     *
     * @param key   Redis键
     * @param hKeys Hash键集合
     * @return Hash对象集合
     */
    public List<Object> hashMultiGet(final String key, final Collection<Object> hKeys) {

        return redisTemplate.opsForHash().multiGet(key, hKeys);
    }

    /**
     * 删除Hash中的数据
     *
     * @param key   Redis键
     * @param hKeys Hash键集合
     * @return Hash对象集合
     */
    public long hashDeleteKeys(final String key, final Collection<Object> hKeys) {
        return redisTemplate.opsForHash().delete(key, hKeys);
    }

    /**
     * 增加Hash中数据的数值，返回当前数值
     * @Author fuGaga
     * @Date 2021/4/13 14:43
     * @param key Redis键
     * @param hKey Hash键
     * @param delta 增量
     * @return long
     **/
    public long hashIncrement(final String key, final String hKey, final long delta){
        return redisTemplate.opsForHash().increment(key, hKey, delta);
    }
    /**
     * 增加Hash中数据的数值，返回当前数值
     * @Author fuGaga
     * @Date 2021/4/13 14:43
     * @param key Redis键
     * @param hKey Hash键
     * @param delta 增量
     * @return long
     **/
    public double hashIncrement(final String key, final String hKey, final double delta){
        return redisTemplate.opsForHash().increment(key, hKey, delta);
    }

    // 存储Set相关操作

    /**
     * 往Set中存入数据
     *
     * @param key    Redis键
     * @param values 值
     * @return 存入的个数
     */
    public long setSet(final String key, final Object... values) {
        Long count = redisTemplate.opsForSet().add(key, values);
        return count == null ? 0 : count;
    }

    /**
     * 删除Set中的数据
     *
     * @param key    Redis键
     * @param values 值
     * @return 移除的个数
     */
    public long setDel(final String key, final Object... values) {
        Long count = redisTemplate.opsForSet().remove(key, values);
        return count == null ? 0 : count;
    }

    /**
     * 获取set中的所有对象
     *
     * @param key Redis键
     * @return set集合
     */
    public Set<Object> getSetAll(final String key) {
        return redisTemplate.opsForSet().members(key);
    }

    /**
     * 获取两个set集合的交集
     * @Author fuGaga
     * @Date 2021/4/13 13:26
     * @param key Redis键
     * @param otherKey 另一个key
     * @return java.util.Set<java.lang.Object>
     **/
    public Set<Object> setIntersect(final String key, final String otherKey){
        return redisTemplate.opsForSet().intersect(key, otherKey);
    }

    /**
     * 获取多个set集合的交集
     * @Author fuGaga
     * @Date 2021/4/13 13:26
     * @param key Redis键
     * @param otherKeys 另一些key
     * @return java.util.Set<java.lang.Object>
     **/
    public Set<Object> setIntersect(final String key, final List<String> otherKeys){
        return redisTemplate.opsForSet().intersect(key, otherKeys);
    }

    /**
     * 获取一个或多个set集合的并集
     * @Author fuGaga
     * @Date 2021/4/13 13:26
     * @param key Redis键
     * @param otherKeys 另一些key
     * @return java.util.Set<java.lang.Object>
     **/
    public Set<Object> setUnion(final String key, final List<String> otherKeys){
        return redisTemplate.opsForSet().union(key, otherKeys);
    }

    /**
     * 获取一个或多个set集合的差集
     * @Author fuGaga
     * @Date 2021/4/13 13:26
     * @param key Redis键
     * @param otherKeys 另一些key
     * @return java.util.Set<java.lang.Object>
     **/
    public Set<Object> setDifference(final String key, final List<String> otherKeys){
        return redisTemplate.opsForSet().difference(key, otherKeys);
    }

    /**
     * 随机获取set集合中count个元素(非去重)
     * @Author fuGaga
     * @Date 2021/4/13 13:26
     * @param key Redis键
     * @param count 元素个数
     * @return java.util.Set<java.lang.Object>
     **/
    public List<Object> setRandomMembers(final String key, final long count){
        return redisTemplate.opsForSet().randomMembers(key, count);
    }

    /**
     * 随机获取set集合中count个元素(去重)
     * @Author fuGaga
     * @Date 2021/4/13 13:26
     * @param key Redis键
     * @param count 元素个数
     * @return java.util.Set<java.lang.Object>
     **/
    public Set<Object> setDistinctRandomMembers(final String key, final long count){
        return redisTemplate.opsForSet().distinctRandomMembers(key, count);
    }


    // 存储ZSet相关操作

    /**
     * 往ZSet中批量存入数据
     *
     * @param key    Redis键
     * @param values 值
     * @return 存入的个数
     */
    public long zsetSet(final String key, final Set<ZSetOperations.TypedTuple<Object>> values) {
        Long count = redisTemplate.opsForZSet().add(key, values);
        return count == null ? 0 : count;
    }

    /**
     * 往ZSet中存入数据
     *
     * @param key   Redis键
     * @param value 值
     * @param score 权重
     * @return true存入成功，false失败
     */
    public boolean zsetSet(final String key, final Object value, double score) {
        Boolean bool = redisTemplate.opsForZSet().add(key, value, score);
        return bool != null && bool;
    }

    /**
     * 获取ZSet中的数据(score从大到小)
     *
     * @param key   Redis键
     * @param start 起始位置
     * @param end   结束位置
     * @return
     */
    public Set<Object> zsetGet(final String key, final long start, final long end) {
        return redisTemplate.opsForZSet().range(key, start, end);
    }


    /**
     * 获取ZSet中的数据(score从小到大)
     *
     * @param key   Redis键
     * @param start 起始位置
     * @param end   结束位置
     * @return
     */
    public Set<Object> zsetReverseGet(final String key, final long start, final long end) {
        return redisTemplate.opsForZSet().reverseRange(key, start, end);
    }

    /**
     * 删除ZSet中的数据
     *
     * @param key    Redis键
     * @param values 值
     * @return 移除的个数
     */
    public long zsetDel(final String key, final Set<ZSetOperations.TypedTuple<Object>> values) {
        Long count = redisTemplate.opsForZSet().remove(key, values);
        return count == null ? 0 : count;
    }

    /**
     * 删除ZSet中的数据
     *
     * @param key    Redis键
     * @param values 值
     * @return 移除的个数
     */
    public long zsetDel(final String key, final Object values) {
        Long count = redisTemplate.opsForZSet().remove(key, values);
        return count == null ? 0 : count;
    }

    /**
     * 增加zset元素的score值，并返回增加后的值
     * @Author fuGaga
     * @Date 2021/4/13 13:48
     * @param key Redis键
     * @param value 元素
     * @param score 增量
     * @return java.lang.Double
     **/
    public Double zsetIncrementScore(final String key, final Object value, final Double score){
        return redisTemplate.opsForZSet().incrementScore(key, value, score);
    }

    /**
     * 返回元素在zset集合的排名
     * @Author fuGaga
     * @Date 2021/4/13 13:53
     * @param [key, value]
     * @return java.lang.Long
     **/
    public Long zsetRank(final String key, final Object value){
        return redisTemplate.opsForZSet().rank(key, value);
    }

    /**
     * 获取指定score范围的有序集合(由score从大到小)
     * @Author fuGaga
     * @Date 2021/4/13 14:14
     * @param [key, min, max]
     * @return java.util.Set<java.lang.Object>
     **/
    public Set<Object> zsetRangeByScore(final String key, final double min, final double max){
        return redisTemplate.opsForZSet().rangeByScore(key, min, max);
    }

    /**
     * 获取指定score范围的有序集合(由score从小到大)
     * @Author fuGaga
     * @Date 2021/4/13 14:14
     * @param [key, min, max]
     * @return java.util.Set<java.lang.Object>
     **/
    public Set<Object> zsetReverseRangeByScore(final String key, final double min, final double max){
        return redisTemplate.opsForZSet().reverseRangeByScore(key, min, max);
    }


    // 存储List相关操作

    /**
     * 往List中存入数据
     *
     * @param key   Redis键
     * @param value 数据
     * @return 存入的个数
     */
    public long listPush(final String key, final Object value) {
        Long count = redisTemplate.opsForList().rightPush(key, value);
        return count == null ? 0 : count;
    }

    /**
     * 往List中存入多个数据
     *
     * @param key    Redis键
     * @param values 多个数据
     * @return 存入的个数
     */
    public long listPushAll(final String key, final Collection<Object> values) {
        Long count = redisTemplate.opsForList().rightPushAll(key, values);
        return count == null ? 0 : count;
    }

    /**
     * 往List中存入多个数据
     *
     * @param key    Redis键
     * @param values 多个数据
     * @return 存入的个数
     */
    public long listPushAll(final String key, final Object... values) {
        Long count = redisTemplate.opsForList().rightPushAll(key, values);
        return count == null ? 0 : count;
    }

    /**
     * 从List中获取begin到end之间的元素
     *
     * @param key   Redis键
     * @param start 开始位置
     * @param end   结束位置（start=0，end=-1表示获取全部元素）
     * @return List对象
     */
    public List<Object> listGet(final String key, final int start, final int end) {
        return redisTemplate.opsForList().range(key, start, end);
    }

    /**
     * 移除并获取List中第一个元素(如果没有元素，一直阻塞)
     * @Author fuGaga
     * @Date 2021/1/4 13:33
     * @Param [key]
     * @Return java.lang.Object
     **/
    public Object listPop(final String key) {
        return redisTemplate.opsForList().leftPop(key);
    }
    /**
     * 移除并获取List中第一个元素(如果没有元素，阻塞直到等待超时)
     * @Author fuGaga
     * @Date 2021/4/13 11:37
     * @param key 键
     * @param timeOut 秒
     * @return java.lang.Object
     **/
    public Object listPopTimeOut(final String key, final long timeOut){
        return redisTemplate.opsForList().leftPop(key, timeOut, TimeUnit.SECONDS);
    }

    /**
     * 发送消息到管道
     *
     * @Author fuGaga
     * @Date 2021/1/4 14:48
     * @Param [cannel, map]
     * @Return void
     **/
    public void sendMessageToCannel(final String cannel, final String message) {
        Map<String, String> map = new HashMap<>();
        map.put("messageId", UUID.randomUUID().toString());
        map.put("message", message);
        redisTemplate.convertAndSend(cannel, map.toString());
    }

    /**
     * 批量插入数据
     *
     * @param keyVals 键值对
     * @param seconds 过期时间(秒)，设置永不过期传-1
     * @return void
     * @Author fuGaga
     * @Date 2021/1/27 17:16
     **/
    public void batchSetKey(final Map<String, String> keyVals, final long seconds) {
        redisTemplate.executePipelined(new RedisCallback<String>() {
            @Override
            public String doInRedis(RedisConnection connection) throws DataAccessException {
                for (Map.Entry<String, String> key : keyVals.entrySet()) {
                    connection.set(key.getKey().getBytes(), key.getValue().getBytes());
                    if (!(-1 == seconds)) {
                        connection.expire(key.getKey().getBytes(), seconds);
                    }
                }
                return null;
            }
        });
    }

    /**
     * 批量获取数据
     *
     * @param keys 集合
     * @return void
     * @Author fuGaga
     * @Date 2021/1/27 22:59
     **/
    public List<Object> batchGetKey(final List<String> keys) {
        List<Object> objectList = redisTemplate.executePipelined(new RedisCallback<String>() {
            @Override
            public String doInRedis(RedisConnection connection) throws DataAccessException {
                for (String key : keys) {
                    connection.get(key.getBytes());
                }
                return null;
            }
        });
        return objectList;
    }
}
