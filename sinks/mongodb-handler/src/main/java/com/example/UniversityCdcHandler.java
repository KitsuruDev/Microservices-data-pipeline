package com.example;

import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import static com.mongodb.client.model.Filters.eq;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonInt32;
import org.bson.BsonArray;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class UniversityCdcHandler extends CdcHandler {

    private final Map<String, BsonDocument> state = new ConcurrentHashMap<>();

    // Конструктор, требуемый для версии 1.8.0
    public UniversityCdcHandler(MongoSinkTopicConfig config) {
        super(config);
    }

    @Override
    public Optional<WriteModel<BsonDocument>> handle(SinkDocument doc) {
        // Извлекаем BsonDocument из сообщения
        BsonDocument valueDoc = doc.getValueDoc().orElse(null);
        if (valueDoc == null) {
            return Optional.empty();
        }

        // Определяем операцию и данные
        String op = valueDoc.getString("op").getValue();
        BsonDocument after = valueDoc.getDocument("after", null);
        BsonDocument before = valueDoc.getDocument("before", null);

        // Имя таблицы по характерным полям
        String table = determineTable(after != null ? after : before);
        if (table == null) return Optional.empty();

        String id = after != null ? after.getString("id").getValue() : before.getString("id").getValue();
        if (id == null) return Optional.empty();

        // Обновляем кэш
        if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
            state.put(table + ":" + id, after);
        } else if (before != null && "d".equals(op)) {
            state.remove(table + ":" + id);
        }

        // Перестраиваем документ университета
        BsonDocument universityDoc = buildUniversityDocument();
        if (universityDoc == null) {
            return Optional.empty();
        }

        // Возвращаем операцию замены (upsert)
        return Optional.of(new ReplaceOneModel<>(
                eq("_id", universityDoc.getString("_id").getValue()),
                universityDoc,
                new ReplaceOptions().upsert(true)
        ));
    }

    private String determineTable(BsonDocument doc) {
        if (doc.containsKey("dean")) return "institute";
        if (doc.containsKey("head")) return "department";
        if (doc.containsKey("degree_level")) return "specialty";
        if (doc.containsKey("is_primary")) return "department_specialties";
        if (doc.containsKey("website")) return "university";
        return null;
    }

    private BsonDocument buildUniversityDocument() {
        // Ищем запись университета (содержит поле 'website')
        BsonDocument univ = null;
        for (BsonDocument doc : state.values()) {
            if (doc.containsKey("website")) {
                univ = doc;
                break;
            }
        }
        if (univ == null) return null;

        // Собираем справочники
        Map<String, BsonDocument> institutes = new HashMap<>();
        Map<String, BsonDocument> departments = new HashMap<>();
        Map<String, BsonDocument> specialties = new HashMap<>();
        java.util.List<BsonDocument> deptSpecs = new java.util.ArrayList<>();

        for (Map.Entry<String, BsonDocument> entry : state.entrySet()) {
            String key = entry.getKey();
            BsonDocument val = entry.getValue();
            if (key.startsWith("institute:")) institutes.put(val.getString("id").getValue(), val);
            else if (key.startsWith("department:")) departments.put(val.getString("id").getValue(), val);
            else if (key.startsWith("specialty:")) specialties.put(val.getString("id").getValue(), val);
            else if (key.startsWith("department_specialties:")) deptSpecs.add(val);
        }

        // Строим иерархию
        BsonArray institutesArray = new BsonArray();
        for (BsonDocument inst : institutes.values()) {
            BsonDocument instDoc = new BsonDocument();
            instDoc.put("_id", inst.getString("id"));
            instDoc.put("university_id", inst.getString("university_id"));
            instDoc.put("name", inst.getString("name"));
            instDoc.put("short_name", inst.getString("short_name"));
            instDoc.put("dean", inst.getString("dean"));

            BsonArray deptArray = new BsonArray();
            for (BsonDocument dept : departments.values()) {
                if (dept.getString("institute_id").getValue().equals(inst.getString("id").getValue())) {
                    BsonDocument deptDoc = new BsonDocument();
                    deptDoc.put("_id", dept.getString("id"));
                    deptDoc.put("institute_id", dept.getString("institute_id"));
                    deptDoc.put("name", dept.getString("name"));
                    deptDoc.put("short_name", dept.getString("short_name"));
                    deptDoc.put("head", dept.getString("head"));
                    deptDoc.put("room", dept.getString("room"));

                    BsonArray specArray = new BsonArray();
                    for (BsonDocument ds : deptSpecs) {
                        if (ds.getString("department_id").getValue().equals(dept.getString("id").getValue())) {
                            String specId = ds.getString("specialty_id").getValue();
                            BsonDocument spec = specialties.get(specId);
                            if (spec != null) {
                                BsonDocument specDoc = new BsonDocument();
                                specDoc.put("id", spec.getString("id"));
                                specDoc.put("name", spec.getString("name"));
                                specDoc.put("code", spec.getString("code"));
                                specDoc.put("degree_level", spec.getString("degree_level"));
                                specArray.add(specDoc);
                            }
                        }
                    }
                    deptDoc.put("specialties", specArray);
                    deptArray.add(deptDoc);
                }
            }
            instDoc.put("departments", deptArray);
            institutesArray.add(instDoc);
        }

        BsonDocument result = new BsonDocument();
        result.put("_id", univ.getString("id"));
        result.put("name", univ.getString("name"));
        result.put("short_name", univ.getString("short_name"));
        result.put("address", univ.getString("address"));
        result.put("website", univ.getString("website"));
        result.put("founded_year", univ.getInt32("founded_year"));
        result.put("institutes", institutesArray);

        return result;
    }
}