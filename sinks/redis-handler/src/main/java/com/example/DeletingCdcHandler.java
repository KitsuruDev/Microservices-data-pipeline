package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DeletingCdcHandler<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(DeletingCdcHandler.class);
    private JedisPool jedisPool;
    // Кэш: id студента (UUID) -> student_card_number (строка)
    private final Map<String, String> studentIdToCard = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        String redisHost = "redis-server";
        int redisPort = 6379;
        int timeout = 2000;
        String password = null;

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(2);

        if (password != null && !password.isEmpty()) {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, timeout, password);
        } else {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, timeout);
        }
        log.info("RedisCdcHandler initialized (Redis at {}:{})", redisHost, redisPort);
    }

    @Override
    public R apply(R record) {
        // tombstone – удаляем ключ из Redis
        if (record.value() == null) {
            String id = extractIdFromKey(record.key());
            if (id != null) {
                String cardNumber = studentIdToCard.remove(id);
                if (cardNumber != null) {
                    String redisKey = "student:" + cardNumber;
                    try (Jedis jedis = jedisPool.getResource()) {
                        Long result = jedis.del(redisKey);
                        log.info("Deleted key '{}' from Redis (result={})", redisKey, result);
                    } catch (Exception e) {
                        log.error("Failed to delete key '{}' from Redis", redisKey, e);
                    }
                } else {
                    log.warn("No cached student_card_number for id {}, cannot delete", id);
                }
            } else {
                log.warn("Tombstone with null/empty key, skipping");
            }
            return null; // не передаём tombstone дальше коннектору
        }

        // для insert/update: обновляем кэш id -> student_card_number
        String cardNumber = extractFieldFromValue(record.value(), "student_card_number");
        String id = extractFieldFromValue(record.value(), "id");
        if (id != null && cardNumber != null) {
            studentIdToCard.put(id, cardNumber);
        }
        // Пропускаем запись без изменений (коннектор сам выполнит UPSERT)
        return record;
    }

    private String extractIdFromKey(Object key) {
        if (key == null) return null;
        String keyStr = key.toString();
        // Извлекаем "id" из JSON: {"schema":{...},"payload":{"id":"..."}}
        return extractJsonStringField(keyStr, "id");
    }

    private String extractFieldFromValue(Object value, String fieldName) {
        if (value == null) return null;
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            try {
                return struct.getString(fieldName);
            } catch (Exception e) {
                return null;
            }
        }
        if (value instanceof Map) {
            return (String) ((Map) value).get(fieldName);
        }
        // значение – строка (JSON)
        return extractJsonStringField(value.toString(), fieldName);
    }

    private String extractJsonStringField(String json, String fieldName) {
        String search = "\"" + fieldName + "\":\"";
        int idx = json.indexOf(search);
        if (idx != -1) {
            int start = idx + search.length();
            int end = json.indexOf('"', start);
            if (end != -1) {
                return json.substring(start, end);
            }
        }
        return null;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}