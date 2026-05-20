package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LectureMaterialCdcHandler<R extends ConnectRecord<R>> implements Transformation<R> {

    // Кэши для хранения последнего состояния справочных данных
    private static final Map<String, Struct> universities = new ConcurrentHashMap<>();
    private static final Map<String, Struct> institutes = new ConcurrentHashMap<>();
    private static final Map<String, Struct> departments = new ConcurrentHashMap<>();
    private static final Map<String, Struct> specialties = new ConcurrentHashMap<>();
    private static final Map<String, Struct> deptSpecs = new ConcurrentHashMap<>(); // key = "deptId:specId"
    private static final Map<String, Struct> lectureCourses = new ConcurrentHashMap<>();
    private static final Map<String, Struct> lectures = new ConcurrentHashMap<>();
    private static final Map<String, Struct> lectureMaterials = new ConcurrentHashMap<>();

    @Override
    public R apply(R record) {
        if (record.value() == null) return null;

        Struct value = (Struct) record.value();
        String table = extractTableName(record.topic());

        // Безопасная проверка, является ли запись tombstone-удалением
        boolean isDeleted = false;
        try {
            // Если поле отсутствует или тип не BOOLEAN, getBoolean выбросит исключение
            isDeleted = Boolean.TRUE.equals(value.getBoolean("__deleted"));
        } catch (Exception ignored) {
            // __deleted отсутствует или имеет неверный тип → не удаление
        }
        if (isDeleted) {
            if ("lecture_material".equals(table)) {
                String id = value.getString("id");
                lectureMaterials.remove(id);
                // tombstone для удаления документа в ES
                return record.newRecord("dbserver.public.lecture_material", record.kafkaPartition(),
                        Schema.STRING_SCHEMA, id, null, null, record.timestamp());
            } else {
                removeFromCache(table, value);
                return null;
            }
        }

        // Вставка/обновление
        updateCache(table, value);
        Set<String> materialIds = getAffectedMaterials(table, value);
        for (String matId : materialIds) {
            Struct mat = lectureMaterials.get(matId);
            if (mat != null) {
                Map<String, Object> doc = buildMaterialDocument(mat);
                if (doc != null) {
                    return record.newRecord("dbserver.public.lecture_material", record.kafkaPartition(),
                            Schema.STRING_SCHEMA, matId, null, doc, record.timestamp());
                }
            }
        }
        return null;
    }

    private void updateCache(String table, Struct record) {
        String id = record.getString("id");
        switch (table) {
            case "university": universities.put(id, record); break;
            case "institute": institutes.put(id, record); break;
            case "department": departments.put(id, record); break;
            case "specialty": specialties.put(id, record); break;
            case "department_specialties": {
                String deptId = record.getString("department_id");
                String specId = record.getString("specialty_id");
                deptSpecs.put(deptId + ":" + specId, record);
                break;
            }
            case "lecture_course": lectureCourses.put(id, record); break;
            case "lecture": lectures.put(id, record); break;
            case "lecture_material": lectureMaterials.put(id, record); break;
        }
    }

    private void removeFromCache(String table, Struct record) {
        String id = record.getString("id");
        switch (table) {
            case "university": universities.remove(id); break;
            case "institute": institutes.remove(id); break;
            case "department": departments.remove(id); break;
            case "specialty": specialties.remove(id); break;
            case "department_specialties": {
                String deptId = record.getString("department_id");
                String specId = record.getString("specialty_id");
                deptSpecs.remove(deptId + ":" + specId);
                break;
            }
            case "lecture_course": lectureCourses.remove(id); break;
            case "lecture": lectures.remove(id); break;
            case "lecture_material": lectureMaterials.remove(id); break;
        }
    }

    private String extractTableName(String topic) {
        return topic.substring(topic.lastIndexOf('.') + 1);
    }

    /**
     * Находит все материалы, затронутые изменениями в указанной таблице.
     * Для полной денормализации нужно пройти по всей цепочке связей,
     * но для демонстрации достаточно прямых зависимостей.
     */
    private Set<String> getAffectedMaterials(String table, Struct value) {
        Set<String> ids = new HashSet<>();
        switch (table) {
            case "lecture_material":
                if (value != null) ids.add(value.getString("id"));
                break;
            case "lecture":
                if (value != null) {
                    String lectureId = value.getString("id");
                    lectureMaterials.forEach((matId, mat) -> {
                        if (lectureId.equals(mat.getString("lecture_id"))) ids.add(matId);
                    });
                }
                break;
            case "lecture_course":
                if (value != null) {
                    String courseId = value.getString("id");
                    lectures.forEach((lecId, lec) -> {
                        if (courseId.equals(lec.getString("course_id"))) {
                            lectureMaterials.forEach((matId, mat) -> {
                                if (lecId.equals(mat.getString("lecture_id"))) ids.add(matId);
                            });
                        }
                    });
                }
                break;
            case "specialty":
                if (value != null) {
                    String specId = value.getString("id");
                    lectureCourses.forEach((courseId, course) -> {
                        if (specId.equals(course.getString("specialty_id"))) {
                            lectures.forEach((lecId, lec) -> {
                                if (courseId.equals(lec.getString("course_id"))) {
                                    lectureMaterials.forEach((matId, mat) -> {
                                        if (lecId.equals(mat.getString("lecture_id"))) ids.add(matId);
                                    });
                                }
                            });
                        }
                    });
                }
                break;
            // Остальные таблицы (department, etc.) пока не влияют на поля course_name/specialty_name
            default:
                // Для department, institute и т.п. можно позже добавить обновление, если понадобится
                break;
        }
        return ids;
    }

    /**
     * Строит документ материала для Elasticsearch строго по заданной схеме.
     */
    private Map<String, Object> buildMaterialDocument(Struct material) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("id", material.getString("id"));
        doc.put("lecture_id", material.getString("lecture_id"));
        doc.put("content_type", material.getString("content_type"));
        doc.put("title", material.getString("title"));
        doc.put("content_text", material.getString("content_text"));
        doc.put("file_url", material.getString("file_url"));

        // metadata в PostgreSQL – JSONB, приходит как строка
        doc.put("metadata", material.getString("metadata"));

        // Debezium MicroTimestamp -> миллисекунды Unix Epoch
        Long createdMicros = material.getInt64("created_at");
        doc.put("created_at", createdMicros != null ? createdMicros / 1000 : null);

        // Собираем course_name и specialty_name по цепочке связей
        String lectureId = material.getString("lecture_id");
        if (lectureId != null) {
            Struct lecture = lectures.get(lectureId);
            if (lecture != null) {
                String courseId = lecture.getString("course_id");
                if (courseId != null) {
                    Struct course = lectureCourses.get(courseId);
                    if (course != null) {
                        doc.put("course_name", course.getString("name"));
                        String specId = course.getString("specialty_id");
                        if (specId != null) {
                            Struct spec = specialties.get(specId);
                            if (spec != null) {
                                doc.put("specialty_name", spec.getString("name"));
                            }
                        }
                    }
                }
            }
        }

        // Если связи не найдены, подставляем null
        doc.putIfAbsent("course_name", null);
        doc.putIfAbsent("specialty_name", null);
        return doc;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}