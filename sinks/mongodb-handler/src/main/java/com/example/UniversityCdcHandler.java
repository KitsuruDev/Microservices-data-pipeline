package com.example;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.mongodb.client.model.Filters.eq;

public class UniversityCdcHandler extends CdcHandler {

    private static final Logger LOG = LoggerFactory.getLogger(UniversityCdcHandler.class);

    private static final Map<String, BsonDocument> universities = new ConcurrentHashMap<>();
    private static final Map<String, BsonDocument> institutes = new ConcurrentHashMap<>();
    private static final Map<String, BsonDocument> departments = new ConcurrentHashMap<>();
    private static final Map<String, BsonDocument> specialties = new ConcurrentHashMap<>();
    private static final List<BsonDocument> deptSpecs = Collections.synchronizedList(new ArrayList<>());

    public UniversityCdcHandler(MongoSinkTopicConfig config) {
        super(config);
        LOG.info("UniversityCdcHandler initialized");
    }

    @Override
    public Optional<WriteModel<BsonDocument>> handle(SinkDocument doc) {
        BsonDocument valueDoc = doc.getValueDoc().orElse(null);
        if (valueDoc == null) {
            LOG.warn("valueDoc is null");
            return Optional.empty();
        }

        BsonValue opVal = valueDoc.get("op");
        if (opVal == null || !opVal.isString()) {
            LOG.warn("Missing or invalid 'op'");
            return Optional.empty();
        }
        String op = opVal.asString().getValue();
        LOG.info("Event op={}", op);

        BsonDocument after = getDocument(valueDoc, "after");
        BsonDocument before = getDocument(valueDoc, "before");

        // Определяем таблицу
        String table = null;
        if (after != null) {
            table = determineTableByFields(after);
            LOG.debug("Table by fields: {}", table);
        }
        if (table == null) {
            String id = extractId(after, before);
            if (id != null) {
                table = findTableInCaches(id);
                LOG.debug("Table by cache for id {}: {}", id, table);
            }
        }
        if (table == null) {
            LOG.warn("Cannot determine table for event");
            return Optional.empty();
        }

        String id = extractId(after, before);
        if (id == null) {
            LOG.error("No id found for table {}", table);
            return Optional.empty();
        }
        LOG.info("Processing table={}, op={}, id={}", table, op, id);

        // Обработка каждой таблицы
        switch (table) {
            case "university":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    universities.put(id, after);
                    LOG.info("Cached university {}", id);
                    BsonDocument docOut = buildUniversityDocument(id);
                    if (docOut != null) {
                        return Optional.of(new ReplaceOneModel<>(eq("_id", docOut.getString("_id").getValue()), docOut, new ReplaceOptions().upsert(true)));
                    } else {
                        LOG.error("Failed to build university document for {}", id);
                        return Optional.empty();
                    }
                } else if (before != null && "d".equals(op)) {
                    universities.remove(id);
                    LOG.info("Deleted university {}, returning DeleteOneModel", id);
                    return Optional.of(new DeleteOneModel<>(eq("_id", new BsonString(id))));
                }
                break;

            case "institute":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    institutes.put(id, after);
                    LOG.info("Cached institute {}", id);
                    String univId = getUniversityIdFromInstitute(after);
                    if (univId != null && universities.containsKey(univId)) {
                        BsonDocument univDoc = buildUniversityDocument(univId);
                        if (univDoc != null) {
                            return Optional.of(new ReplaceOneModel<>(eq("_id", univDoc.getString("_id").getValue()), univDoc, new ReplaceOptions().upsert(true)));
                        }
                    }
                } else if (before != null && "d".equals(op)) {
                    BsonDocument cached = institutes.remove(id);
                    LOG.info("Deleted institute {}", id);
                    String univId = (cached != null) ? getUniversityIdFromInstitute(cached) : null;
                    if (univId != null && universities.containsKey(univId)) {
                        BsonDocument univDoc = buildUniversityDocument(univId);
                        if (univDoc != null) {
                            return Optional.of(new ReplaceOneModel<>(eq("_id", univDoc.getString("_id").getValue()), univDoc, new ReplaceOptions().upsert(true)));
                        }
                    }
                }
                break;

            case "department":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    departments.put(id, after);
                    LOG.info("Cached department {}", id);
                    String univId = getUniversityIdFromDepartment(after);
                    if (univId != null && universities.containsKey(univId)) {
                        BsonDocument univDoc = buildUniversityDocument(univId);
                        if (univDoc != null) {
                            return Optional.of(new ReplaceOneModel<>(eq("_id", univDoc.getString("_id").getValue()), univDoc, new ReplaceOptions().upsert(true)));
                        }
                    }
                } else if (before != null && "d".equals(op)) {
                    BsonDocument cached = departments.remove(id);
                    LOG.info("Deleted department {}", id);
                    String univId = (cached != null) ? getUniversityIdFromDepartment(cached) : null;
                    if (univId != null && universities.containsKey(univId)) {
                        BsonDocument univDoc = buildUniversityDocument(univId);
                        if (univDoc != null) {
                            return Optional.of(new ReplaceOneModel<>(eq("_id", univDoc.getString("_id").getValue()), univDoc, new ReplaceOptions().upsert(true)));
                        }
                    }
                }
                break;

            case "specialty":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    specialties.put(id, after);
                    LOG.info("Cached specialty {}", id);
                    // Обновляем все университеты, где есть эта специальность
                    for (String univId : universities.keySet()) {
                        BsonDocument univDoc = buildUniversityDocument(univId);
                        if (univDoc != null) {
                            return Optional.of(new ReplaceOneModel<>(eq("_id", univDoc.getString("_id").getValue()), univDoc, new ReplaceOptions().upsert(true)));
                        }
                    }
                } else if (before != null && "d".equals(op)) {
                    specialties.remove(id);
                    LOG.info("Deleted specialty {}", id);
                    for (String univId : universities.keySet()) {
                        BsonDocument univDoc = buildUniversityDocument(univId);
                        if (univDoc != null) {
                            return Optional.of(new ReplaceOneModel<>(eq("_id", univDoc.getString("_id").getValue()), univDoc, new ReplaceOptions().upsert(true)));
                        }
                    }
                }
                break;

            case "department_specialties":
                if (after != null && ("c".equals(op) || "r".equals(op) || "u".equals(op))) {
                    // Обновляем или добавляем связь
                    String dsId = after.getString("id").getValue();
                    deptSpecs.removeIf(d -> dsId.equals(d.getString("id").getValue()));
                    deptSpecs.add(after);
                    LOG.info("Cached department_specialties {}", dsId);
                    String univId = getUniversityIdFromDeptSpec(after);
                    if (univId != null && universities.containsKey(univId)) {
                        BsonDocument univDoc = buildUniversityDocument(univId);
                        if (univDoc != null) {
                            return Optional.of(new ReplaceOneModel<>(eq("_id", univDoc.getString("_id").getValue()), univDoc, new ReplaceOptions().upsert(true)));
                        }
                    }
                } else if (before != null && "d".equals(op)) {
                    String dsId = before.getString("id").getValue();
                    BsonDocument toRemove = deptSpecs.stream().filter(d -> dsId.equals(d.getString("id").getValue())).findFirst().orElse(null);
                    deptSpecs.removeIf(d -> dsId.equals(d.getString("id").getValue()));
                    LOG.info("Deleted department_specialties {}", dsId);
                    if (toRemove != null) {
                        String univId = getUniversityIdFromDeptSpec(toRemove);
                        if (univId != null && universities.containsKey(univId)) {
                            BsonDocument univDoc = buildUniversityDocument(univId);
                            if (univDoc != null) {
                                return Optional.of(new ReplaceOneModel<>(eq("_id", univDoc.getString("_id").getValue()), univDoc, new ReplaceOptions().upsert(true)));
                            }
                        }
                    }
                }
                break;
        }

        LOG.info("No write model produced for event table={}, op={}, id={}", table, op, id);
        return Optional.empty();
    }

    private BsonDocument getDocument(BsonDocument valueDoc, String field) {
        BsonValue val = valueDoc.get(field);
        return (val != null && val.isDocument()) ? val.asDocument() : null;
    }

    private String extractId(BsonDocument after, BsonDocument before) {
        BsonDocument target = after != null ? after : before;
        if (target == null) return null;
        BsonValue idVal = target.get("id");
        return (idVal != null && idVal.isString()) ? idVal.asString().getValue() : null;
    }

    private String determineTableByFields(BsonDocument doc) {
        if (doc.containsKey("dean")) return "institute";
        if (doc.containsKey("head")) return "department";
        if (doc.containsKey("degree_level")) return "specialty";
        if (doc.containsKey("is_primary")) return "department_specialties";
        if (doc.containsKey("website")) return "university";
        return null;
    }

    private String findTableInCaches(String id) {
        if (universities.containsKey(id)) return "university";
        if (institutes.containsKey(id)) return "institute";
        if (departments.containsKey(id)) return "department";
        if (specialties.containsKey(id)) return "specialty";
        if (deptSpecs.stream().anyMatch(d -> id.equals(d.getString("id").getValue()))) return "department_specialties";
        return null;
    }

    private String getUniversityIdFromInstitute(BsonDocument inst) {
        BsonValue univId = inst.get("university_id");
        return (univId != null && univId.isString()) ? univId.asString().getValue() : null;
    }

    private String getUniversityIdFromDepartment(BsonDocument dept) {
        BsonValue instIdVal = dept.get("institute_id");
        if (instIdVal == null || !instIdVal.isString()) return null;
        BsonDocument inst = institutes.get(instIdVal.asString().getValue());
        if (inst == null) return null;
        BsonValue univId = inst.get("university_id");
        return (univId != null && univId.isString()) ? univId.asString().getValue() : null;
    }

    private String getUniversityIdFromDeptSpec(BsonDocument deptSpec) {
        BsonValue deptIdVal = deptSpec.get("department_id");
        if (deptIdVal == null || !deptIdVal.isString()) return null;
        BsonDocument dept = departments.get(deptIdVal.asString().getValue());
        if (dept == null) return null;
        BsonValue instIdVal = dept.get("institute_id");
        if (instIdVal == null || !instIdVal.isString()) return null;
        BsonDocument inst = institutes.get(instIdVal.asString().getValue());
        if (inst == null) return null;
        BsonValue univId = inst.get("university_id");
        return (univId != null && univId.isString()) ? univId.asString().getValue() : null;
    }

    private BsonDocument buildUniversityDocument(String universityId) {
        BsonDocument univ = universities.get(universityId);
        if (univ == null) {
            LOG.error("University {} not found in cache", universityId);
            return null;
        }

        BsonDocument result = new BsonDocument();
        result.put("_id", univ.getString("id"));
        result.put("name", univ.getString("name"));
        result.put("short_name", univ.getString("short_name"));
        result.put("address", univ.getString("address"));
        result.put("website", univ.getString("website"));
        BsonValue yearVal = univ.get("founded_year");
        result.put("founded_year", (yearVal != null && yearVal.isInt32()) ? yearVal.asInt32() : new BsonInt32(0));

        BsonArray institutesArray = new BsonArray();
        for (BsonDocument inst : institutes.values()) {
            if (!universityId.equals(getUniversityIdFromInstitute(inst))) continue;

            BsonDocument instDoc = new BsonDocument();
            instDoc.put("_id", inst.getString("id"));
            instDoc.put("university_id", inst.getString("university_id"));
            instDoc.put("name", inst.getString("name"));
            instDoc.put("short_name", inst.getString("short_name"));
            instDoc.put("dean", inst.getString("dean"));

            BsonArray deptArray = new BsonArray();
            for (BsonDocument dept : departments.values()) {
                BsonValue deptInstIdVal = dept.get("institute_id");
                if (deptInstIdVal == null || !deptInstIdVal.isString()) continue;
                if (!inst.getString("id").getValue().equals(deptInstIdVal.asString().getValue())) continue;

                BsonDocument deptDoc = new BsonDocument();
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
                    if (dept.getString("id").getValue().equals(dsDeptIdVal.asString().getValue())) {
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