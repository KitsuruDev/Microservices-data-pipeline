package com.example;

import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.DeleteOneModel;
import static com.mongodb.client.model.Filters.eq;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonInt32;
import org.bson.BsonArray;
import org.bson.BsonValue;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UniversityCdcHandler extends CdcHandler {

    // СТАТИЧЕСКИЕ кэши – состояние теперь общее для всех экземпляров
    private static final Map<String, BsonDocument> universities = new ConcurrentHashMap<>();
    private static final Map<String, BsonDocument> institutes = new ConcurrentHashMap<>();
    private static final Map<String, BsonDocument> departments = new ConcurrentHashMap<>();
    private static final Map<String, BsonDocument> specialties = new ConcurrentHashMap<>();
    private static final List<BsonDocument> deptSpecs = Collections.synchronizedList(new ArrayList<>());

    public UniversityCdcHandler(MongoSinkTopicConfig config) {
        super(config);
    }

    @Override
    public Optional<WriteModel<BsonDocument>> handle(SinkDocument doc) {
        BsonDocument valueDoc = doc.getValueDoc().orElse(null);
        if (valueDoc == null) return Optional.empty();

        BsonValue opVal = valueDoc.get("op");
        if (opVal == null || !opVal.isString()) return Optional.empty();
        String op = opVal.asString().getValue();

        BsonValue afterVal = valueDoc.get("after");
        BsonDocument after = (afterVal != null && afterVal.isDocument()) ? afterVal.asDocument() : null;
        BsonValue beforeVal = valueDoc.get("before");
        BsonDocument before = (beforeVal != null && beforeVal.isDocument()) ? beforeVal.asDocument() : null;

        String table = determineTable(after != null ? after : before);
        if (table == null) return Optional.empty();

        BsonDocument target = after != null ? after : before;
        BsonValue idVal = target.get("id");
        if (idVal == null || !idVal.isString()) return Optional.empty();
        String id = idVal.asString().getValue();

        Set<String> affectedUniversities = new HashSet<>();

        switch (table) {
            case "university":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    universities.put(id, after);
                    affectedUniversities.add(id);
                } else if (before != null && "d".equals(op)) {
                    universities.remove(id);
                    return Optional.of(new DeleteOneModel<>(eq("_id", new BsonString(id))));
                }
                break;

            case "institute":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    institutes.put(id, after);
                    BsonValue univIdVal = after.get("university_id");
                    if (univIdVal != null && univIdVal.isString()) {
                        String univId = univIdVal.asString().getValue();
                        if (universities.containsKey(univId)) affectedUniversities.add(univId);
                    }
                } else if (before != null && "d".equals(op)) {
                    institutes.remove(id);
                    BsonValue univIdVal = before.get("university_id");
                    if (univIdVal != null && univIdVal.isString()) {
                        String univId = univIdVal.asString().getValue();
                        if (universities.containsKey(univId)) affectedUniversities.add(univId);
                    }
                }
                break;

            case "department":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    departments.put(id, after);
                    BsonValue instIdVal = after.get("institute_id");
                    if (instIdVal != null && instIdVal.isString()) {
                        BsonDocument inst = institutes.get(instIdVal.asString().getValue());
                        if (inst != null) {
                            BsonValue univIdVal = inst.get("university_id");
                            if (univIdVal != null && univIdVal.isString()) {
                                String univId = univIdVal.asString().getValue();
                                if (universities.containsKey(univId)) affectedUniversities.add(univId);
                            }
                        }
                    }
                } else if (before != null && "d".equals(op)) {
                    departments.remove(id);
                    BsonValue instIdVal = before.get("institute_id");
                    if (instIdVal != null && instIdVal.isString()) {
                        BsonDocument inst = institutes.get(instIdVal.asString().getValue());
                        if (inst != null) {
                            BsonValue univIdVal = inst.get("university_id");
                            if (univIdVal != null && univIdVal.isString()) {
                                String univId = univIdVal.asString().getValue();
                                if (universities.containsKey(univId)) affectedUniversities.add(univId);
                            }
                        }
                    }
                }
                break;

            case "specialty":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    specialties.put(id, after);
                    affectedUniversities.addAll(universities.keySet());
                } else if (before != null && "d".equals(op)) {
                    specialties.remove(id);
                    affectedUniversities.addAll(universities.keySet());
                }
                break;

            case "department_specialties":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    String dsId = after.getString("id").getValue();
                    deptSpecs.removeIf(d -> d.getString("id").getValue().equals(dsId));
                    deptSpecs.add(after);
                    BsonValue deptIdVal = after.get("department_id");
                    if (deptIdVal != null && deptIdVal.isString()) {
                        BsonDocument dept = departments.get(deptIdVal.asString().getValue());
                        if (dept != null) {
                            BsonValue instIdVal = dept.get("institute_id");
                            if (instIdVal != null && instIdVal.isString()) {
                                BsonDocument inst = institutes.get(instIdVal.asString().getValue());
                                if (inst != null) {
                                    BsonValue univIdVal = inst.get("university_id");
                                    if (univIdVal != null && univIdVal.isString()) {
                                        String univId = univIdVal.asString().getValue();
                                        if (universities.containsKey(univId)) {
                                            affectedUniversities.add(univId);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else if (before != null && "d".equals(op)) {
                    String dsId = before.getString("id").getValue();
                    // Получаем полную запись из кэша, т.к. before содержит только PK
                    BsonDocument cachedDs = deptSpecs.stream()
                        .filter(d -> d.getString("id").getValue().equals(dsId))
                        .findFirst().orElse(null);
                    deptSpecs.removeIf(d -> d.getString("id").getValue().equals(dsId));

                    BsonValue deptIdVal = (cachedDs != null) ? cachedDs.get("department_id") : null;
                    if (deptIdVal != null && deptIdVal.isString()) {
                        String deptId = deptIdVal.asString().getValue();
                        BsonDocument dept = departments.get(deptId);
                        if (dept != null) {
                            BsonValue instIdVal = dept.get("institute_id");
                            if (instIdVal != null && instIdVal.isString()) {
                                String instId = instIdVal.asString().getValue();
                                BsonDocument inst = institutes.get(instId);
                                if (inst != null) {
                                    BsonValue univIdVal = inst.get("university_id");
                                    if (univIdVal != null && univIdVal.isString()) {
                                        String univId = univIdVal.asString().getValue();
                                        if (universities.containsKey(univId)) {
                                            affectedUniversities.add(univId);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                break;
        }

        for (String univId : affectedUniversities) {
            BsonDocument univDoc = buildUniversityDocument(univId);
            if (univDoc != null) {
                return Optional.of(new ReplaceOneModel<>(
                        eq("_id", univDoc.getString("_id").getValue()),
                        univDoc,
                        new ReplaceOptions().upsert(true)
                ));
            }
        }
        return Optional.empty();
    }

    private String determineTable(BsonDocument doc) {
        if (doc.containsKey("dean")) return "institute";
        if (doc.containsKey("head")) return "department";
        if (doc.containsKey("degree_level")) return "specialty";
        if (doc.containsKey("is_primary")) return "department_specialties";
        if (doc.containsKey("website")) return "university";
        return null;
    }

    private BsonDocument buildUniversityDocument(String universityId) {
        BsonDocument univ = universities.get(universityId);
        if (univ == null) return null;

        BsonDocument result = new BsonDocument();
        result.put("_id", univ.getString("id"));
        result.put("name", univ.getString("name"));
        result.put("short_name", univ.getString("short_name"));
        result.put("address", univ.getString("address"));
        result.put("website", univ.getString("website"));
        BsonValue yearVal = univ.get("founded_year");
        if (yearVal != null && yearVal.isInt32()) {
            result.put("founded_year", yearVal.asInt32());
        } else {
            result.put("founded_year", new BsonInt32(0));
        }

        BsonArray institutesArray = new BsonArray();
        for (BsonDocument inst : institutes.values()) {
            BsonValue instUnivIdVal = inst.get("university_id");
            if (instUnivIdVal == null || !instUnivIdVal.isString()) continue;
            if (!universityId.equals(instUnivIdVal.asString().getValue())) continue;

            BsonDocument instDoc = new BsonDocument();
            String instId = inst.getString("id").getValue();
            instDoc.put("_id", inst.getString("id"));
            instDoc.put("university_id", inst.getString("university_id"));
            instDoc.put("name", inst.getString("name"));
            instDoc.put("short_name", inst.getString("short_name"));
            instDoc.put("dean", inst.getString("dean"));

            BsonArray deptArray = new BsonArray();
            for (BsonDocument dept : departments.values()) {
                BsonValue deptInstIdVal = dept.get("institute_id");
                if (deptInstIdVal == null || !deptInstIdVal.isString()) continue;
                if (!instId.equals(deptInstIdVal.asString().getValue())) continue;

                BsonDocument deptDoc = new BsonDocument();
                String deptId = dept.getString("id").getValue();
                deptDoc.put("_id", dept.getString("id"));
                deptDoc.put("institute_id", dept.getString("institute_id"));
                deptDoc.put("name", dept.getString("name"));
                deptDoc.put("short_name", dept.getString("short_name"));
                deptDoc.put("head", dept.getString("head"));
                deptDoc.put("room", dept.getString("room"));

                Set<String> addedSpecIds = new HashSet<>();
                BsonArray specArray = new BsonArray();
                for (BsonDocument ds : deptSpecs) {
                    BsonValue dsDeptIdVal = ds.get("department_id");
                    if (dsDeptIdVal == null || !dsDeptIdVal.isString()) continue;
                    if (deptId.equals(dsDeptIdVal.asString().getValue())) {
                        BsonValue specIdVal = ds.get("specialty_id");
                        if (specIdVal == null || !specIdVal.isString()) continue;
                        String specId = specIdVal.asString().getValue();
                        if (addedSpecIds.add(specId)) {
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
                }
                deptDoc.put("specialties", specArray);
                deptArray.add(deptDoc);
            }
            instDoc.put("departments", deptArray);
            institutesArray.add(instDoc);
        }
        result.put("institutes", institutesArray);
        return result;
    }
}