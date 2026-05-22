package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LectureMaterialCdcHandler<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(LectureMaterialCdcHandler.class);

    private static final Map<String, Struct> lectureCourses = new ConcurrentHashMap<>();
    private static final Map<String, Struct> lectures = new ConcurrentHashMap<>();
    private static final Map<String, Struct> specialties = new ConcurrentHashMap<>();
    private static final Map<String, Struct> lectureMaterials = new ConcurrentHashMap<>();

    @Override
    public R apply(R record) {
        if (record.value() == null) return null;

        Struct value = (Struct) record.value();
        String table = extractTableName(record.topic());

        // Удаление
        boolean isDeleted = false;
        try { isDeleted = Boolean.TRUE.equals(value.getBoolean("__deleted")); } catch (Exception ignored) {}
        if (isDeleted) {
            if ("lecture_material".equals(table)) {
                String id = value.getString("id");
                lectureMaterials.remove(id);
                log.info("Deleted material: {}", id);
                return record.newRecord("dbserver.public.lecture_material", record.kafkaPartition(),
                        Schema.STRING_SCHEMA, id, null, null, record.timestamp());
            } else {
                removeFromCache(table, value);
                return null;
            }
        }

        // Обновление кэша
        updateCache(table, value);

        // Триггерная отправка
        if ("lecture_material".equals(table)) {
            // Новый или обновлённый материал
            String matId = value.getString("id");
            Map<String, Object> doc = buildMaterialDocument(value);
            if (isComplete(doc)) {
                log.info("Sending material {}", matId);
                return record.newRecord("dbserver.public.lecture_material", record.kafkaPartition(),
                        Schema.STRING_SCHEMA, matId, null, doc, record.timestamp());
            }
        } else if ("lecture_course".equals(table)) {
            // Изменился курс — обновляем все его материалы
            String courseId = value.getString("id");
            for (Map.Entry<String, Struct> entry : lectureMaterials.entrySet()) {
                String matId = entry.getKey();
                Struct mat = entry.getValue();
                Map<String, Object> doc = buildMaterialDocument(mat);
                if (courseId.equals(getCourseIdForMaterial(mat))) {
                    log.info("Updating material {} due to course change", matId);
                    return record.newRecord("dbserver.public.lecture_material", record.kafkaPartition(),
                            Schema.STRING_SCHEMA, matId, null, doc, record.timestamp());
                }
            }
        } else if ("lecture".equals(table)) {
            // Изменилась лекция — обновляем её материалы
            String lectureId = value.getString("id");
            for (Map.Entry<String, Struct> entry : lectureMaterials.entrySet()) {
                String matId = entry.getKey();
                Struct mat = entry.getValue();
                if (lectureId.equals(mat.getString("lecture_id"))) {
                    Map<String, Object> doc = buildMaterialDocument(mat);
                    log.info("Updating material {} due to lecture change", matId);
                    return record.newRecord("dbserver.public.lecture_material", record.kafkaPartition(),
                            Schema.STRING_SCHEMA, matId, null, doc, record.timestamp());
                }
            }
        }

        return null;
    }

    private String getCourseIdForMaterial(Struct material) {
        String lectureId = material.getString("lecture_id");
        if (lectureId != null) {
            Struct lecture = lectures.get(lectureId);
            if (lecture != null) {
                return lecture.getString("course_id");
            }
        }
        return null;
    }

    private boolean isComplete(Map<String, Object> doc) {
        return doc.get("course_name") != null;
    }

    private void updateCache(String table, Struct record) {
        String id = record.getString("id");
        switch (table) {
            case "specialty": specialties.put(id, record); break;
            case "lecture_course": lectureCourses.put(id, record); break;
            case "lecture": lectures.put(id, record); break;
            case "lecture_material": lectureMaterials.put(id, record); break;
        }
    }

    private void removeFromCache(String table, Struct record) {
        String id = record.getString("id");
        switch (table) {
            case "specialty": specialties.remove(id); break;
            case "lecture_course": lectureCourses.remove(id); break;
            case "lecture": lectures.remove(id); break;
            case "lecture_material": lectureMaterials.remove(id); break;
        }
    }

    private String extractTableName(String topic) {
        return topic.substring(topic.lastIndexOf('.') + 1);
    }

    private Map<String, Object> buildMaterialDocument(Struct material) {
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

        String lectureId = material.getString("lecture_id");
        if (lectureId != null) {
            Struct lecture = lectures.get(lectureId);
            if (lecture != null) {
                String courseId = lecture.getString("course_id");
                if (courseId != null) {
                    Struct course = lectureCourses.get(courseId);
                    if (course != null) {
                        doc.put("course_name", course.getString("name"));
                    }
                }
            }
        }
        return doc;
    }

    @Override public ConfigDef config() { return new ConfigDef(); }
    @Override public void close() {}
    @Override public void configure(Map<String, ?> configs) {}
}