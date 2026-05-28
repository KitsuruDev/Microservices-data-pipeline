package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

public class LectureMaterialCdcHandler<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(LectureMaterialCdcHandler.class);

    @Override
    public R apply(R record) {
        // Tombstone - удаление документа из Elasticsearch
        if (record.value() == null) {
            String topic = record.topic();
            if (topic != null && topic.endsWith("lecture_material")) {
                String id = extractIdFromKey(record.key());
                if (id != null) {
                    deleteFromElasticsearch(id);
                    log.info("Deleted material {} from Elasticsearch", id);
                }
            }
            return null; // не передаём tombstone дальше
        }

        // Обрабатываем только записи из топика lecture_material
        String topic = record.topic();
        if (topic == null || !topic.endsWith("lecture_material")) {
            return null; // игнорируем другие топики
        }

        Struct value = (Struct) record.value();
        String materialId = value.getString("id");

        // Строим документ для Elasticsearch без обогащения
        Map<String, Object> doc = buildDocument(value);

        // Отправляем в Elasticsearch через коннектор
        log.info("Sending material {} to Elasticsearch", materialId);
        return record.newRecord(
                "dbserver.public.lecture_material",  // выходной топик
                record.kafkaPartition(),
                Schema.STRING_SCHEMA,                // key schema
                materialId,                          // key
                null,                                // value schema (null = нет схемы)
                doc,                                 // значение - документ для ES
                record.timestamp()
        );
    }

    private String extractIdFromKey(Object key) {
        if (key == null) return null;
        if (key instanceof Struct) {
            return ((Struct) key).getString("id");
        }
        if (key instanceof Map) {
            return (String) ((Map<?, ?>) key).get("id");
        }
        // JSON-строка - упрощённый парсинг
        String keyStr = key.toString();
        String search = "\"id\":\"";
        int idx = keyStr.indexOf(search);
        if (idx != -1) {
            int start = idx + search.length();
            int end = keyStr.indexOf('"', start);
            if (end != -1) {
                return keyStr.substring(start, end);
            }
        }
        return keyStr; // fallback
    }

    private void deleteFromElasticsearch(String id) {
        try {
            URL url = new URL("http://elasticsearch-server:9200/lecture_material/_doc/" + id);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            conn.setRequestProperty("Authorization", "Basic " +
                    Base64.getEncoder().encodeToString("elastic:elastic_pass123".getBytes()));
            int code = conn.getResponseCode();
            if (code != 200 && code != 404) {
                log.warn("Elasticsearch DELETE returned {}", code);
            }
            conn.disconnect();
        } catch (Exception e) {
            log.error("Failed to delete from Elasticsearch", e);
        }
    }

    private Map<String, Object> buildDocument(Struct material) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("id", material.getString("id"));
        doc.put("lecture_id", material.getString("lecture_id"));
        doc.put("content_type", material.getString("content_type"));
        doc.put("title", material.getString("title"));
        doc.put("content_text", material.getString("content_text"));
        doc.put("file_url", material.getString("file_url"));
        doc.put("metadata", material.getString("metadata"));

        Long createdMicros = material.getInt64("created_at");
        doc.put("created_at", createdMicros != null ? createdMicros / 1000 : null);

        return doc;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}