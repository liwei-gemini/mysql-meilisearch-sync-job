package org.example;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RecordEnricherFlatMapFunction extends RichFlatMapFunction<String, JSONObject> {

    private final String mysqlHost;
    private final int mysqlPort;
    private final String mysqlDatabase;
    private final String mysqlUser;
    private final String mysqlPassword;
    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;

    // 索引配置字段
    private final String indexPatient;
    private final String indexFollowup;
    private final String indexBedside;
    private final String indexPatientOrg;
    private final String indexSysUser;
    private final String indexSysOrg;

    private transient Connection connection;
    private transient Jedis jedis;

    public RecordEnricherFlatMapFunction(String mysqlHost, int mysqlPort, String mysqlDatabase, String mysqlUser,
            String mysqlPassword, String redisHost, int redisPort, String redisPassword,
            String indexPatient, String indexFollowup, String indexBedside, String indexPatientOrg,
            String indexSysUser, String indexSysOrg) {
        this.mysqlHost = mysqlHost;
        this.mysqlPort = mysqlPort;
        this.mysqlDatabase = mysqlDatabase;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.indexPatient = indexPatient;
        this.indexFollowup = indexFollowup;
        this.indexBedside = indexBedside;
        this.indexPatientOrg = indexPatientOrg;
        this.indexSysUser = indexSysUser;
        this.indexSysOrg = indexSysOrg;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // MySQL连接
        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&characterEncoding=utf-8",
                mysqlHost, mysqlPort, mysqlDatabase);
        connection = DriverManager.getConnection(url, mysqlUser, mysqlPassword);
        System.out.println("Enricher MySQL connection successful: " + url);
        
        // Redis连接
        jedis = new Jedis(redisHost, redisPort, 5000);
        if (redisPassword != null && !redisPassword.isEmpty()) {
            jedis.auth(redisPassword);
        }
        System.out.println("Redis connection successful: " + redisHost + ":" + redisPort);
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
        JSONObject json = JSONObject.parseObject(value);
        JSONObject source = json.getJSONObject("source");
        if (source == null) {
            out.collect(json);
            return;
        }

        String table = source.getString("table");
        boolean isPatientOrg = false;
        boolean isOhScreening = false;
        boolean isSysUser = false;
        boolean isSysOrg = false;

        // 匹配逻辑优化，支持含前缀的表名，并设置对应索引
        if (table != null) {
            if (table.endsWith("bu_follow_up_plan")) {
                enrichBuFollowUpPlan(json);
                json.put("_meili_index", indexFollowup);
            } else if (table.endsWith("bu_patient")) {
                String op = json.getString("op");
                // 仅在新增(c) or 快照读(r) 时初始化 orgList，避免 update(u) 时覆盖已有数据
                if (("c".equals(op) || "r".equals(op)) && json.containsKey("after")) {
                    json.getJSONObject("after").put("orgList", new ArrayList<>());
                }
                json.put("_meili_index", indexPatient);
            } else if (table.endsWith("bu_bedside")) {
                json.put("_meili_index", indexBedside);
            } else if (table.endsWith("bu_patient_org")) {
                isPatientOrg = true;
                enrichBuPatientOrg(json);
                json.put("_meili_index", indexPatientOrg);
            } else if (table.endsWith("bu_oh_screening_interventions_info")) {
                isOhScreening = true;
                json.put("_meili_index", "buOhScreeningInterventionsInfo"); // 设置对应的索引名
            } else if (table.endsWith("sys_user")) {
                isSysUser = true;
                enrichSysUser(json);
                json.put("_meili_index", indexSysUser);
            } else if (table.endsWith("sys_org")) {
                isSysOrg = true;
                enrichSysOrg(json);
                json.put("_meili_index", indexSysOrg);
            }
        }

        // [修复] 先处理建档时间更新，再执行字段名转换
        // 如果是 bu_oh_screening_interventions_info 表的变动，先处理建档时间逻辑
        if (isOhScreening) {
            // [重要修复] 只处理recordTime更新，不发送原始记录
            handlePatientRecordTimeUpdate(json, out);
        }

        // 字段名转换：下划线转驼峰
        convertFieldNamesToCamelCase(json);

        // 通用日期格式化
        formatDatesInCamelCaseMap(json);

        // [修复] 如果是bu_oh_screening_interventions_info表，只发送到正确的索引，不发送到buPatients
        if (!isOhScreening && !shouldSkipSync(json)) {
            // 发送原始（丰富后）的记录
            out.collect(json);
        }

        // 如果是 bu_patient_org 表的变动，额外发送 buPatients 索引的更新
        if (isPatientOrg) {
            emitPatientOrgListUpdate(json, out);
        }
    }



    // [新增] 获取原始患者数据的方法
    private JSONObject getOriginalPatientData(String patientId) {
        try {
            // 从bu_patient表获取原始患者数据
            String sql = "SELECT * FROM bu_patient WHERE id = ?";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, patientId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        JSONObject patientData = new JSONObject();
                        // 获取所有字段
                        java.sql.ResultSetMetaData metaData = rs.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            Object value = rs.getObject(i);
                            patientData.put(columnName, value);
                        }
                        return patientData;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error getting original patient data for " + patientId + ": " + e.getMessage());
        }
        return null;
    }
    // [优化] 获取当前患者数据的方法 - 从Redis缓存获取
    private JSONObject getCurrentPatientData(String patientId) {
        try {
            String redisKey = "patient:" + patientId;
            String cachedData = jedis.get(redisKey);
            
            if (cachedData != null && !cachedData.isEmpty()) {
                JSONObject cached = JSONObject.parseObject(cachedData);
                // 确保缓存数据包含必要的字段
                if (!cached.containsKey("recordTime")) {
                    cached.put("recordTime", new ArrayList<String>());
                }
                if (!cached.containsKey("orgList")) {
                    cached.put("orgList", new ArrayList<String>());
                }
                return cached;
            }
            
            // 如果Redis中没有缓存，从数据库获取基础数据
            JSONObject baseData = getOriginalPatientData(patientId);
            if (baseData != null) {
                // 初始化recordTime和orgList字段
                if (!baseData.containsKey("recordTime")) {
                    baseData.put("recordTime", new ArrayList<String>());
                }
                if (!baseData.containsKey("orgList")) {
                    baseData.put("orgList", new ArrayList<String>());
                }
                // 立即缓存到Redis
                updateRedisCache(patientId, baseData);
                return baseData;
            }
            
        } catch (Exception e) {
            System.err.println("Error getting current patient data for " + patientId + ": " + e.getMessage());
        }
        return new JSONObject(); // 返回空对象而不是null
    }
    // [修改] 修改方法参数，与调用保持一致
    private void emitPatientOrgListUpdate(JSONObject json, Collector<JSONObject> out) {
        JSONObject after = json.getJSONObject("after");
        if (after == null) return;

        // [修改] 字段名已经转换为驼峰命名，应该使用patientId而不是patient_id
        String patientId = after.getString("patientId");
        if (patientId != null && !patientId.isEmpty()) {
            try {
                // [修改] 获取该患者的所有orgList
                List<Long> orgList = new ArrayList<>();
                String sql = "SELECT create_org FROM bu_patient_org WHERE patient_id = ?";
                try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setString(1, patientId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            orgList.add(rs.getLong("create_org"));
                        }
                    }
                }

                // [修改] 将Long列表转换为String列表，以便Meilisearch可以正确filter搜索
                List<String> orgListString = new ArrayList<>();
                for (Long orgId : orgList) {
                    orgListString.add(orgId.toString());
                }

                // [修改] 构造更新 buPatients 的消息，保留原始字段并新增orgList
                JSONObject updateDoc = new JSONObject();
                updateDoc.put("_meili_index", indexPatient);

                // [修改] 从Redis获取当前患者数据
                JSONObject currentPatientData = getCurrentPatientData(patientId);
                JSONObject content = new JSONObject(currentPatientData);

                // [修改] 新增或更新orgList字段，使用字符串列表
                content.put("id", patientId);
                content.put("orgList", orgListString);

                // [新增] 将content放入after字段，并调用字段名转换方法
                updateDoc.put("after", content);
                convertFieldNamesToCamelCase(updateDoc);

                // 构造 update 类型的 CDC 结构，以便 Sink 处理
                // MeilisearchSink 会直接取 after 内容进行 upsert
                updateDoc.put("op", "u");

                // 发送到Meilisearch
                out.collect(updateDoc);

                // [新增] 更新Redis缓存
                updateRedisCache(patientId, content);

            } catch (Exception e) {
                System.err.println("Error enriching orgList for patient " + patientId + ": " + e.getMessage());
            }
        } else {
            System.err.println("Error: patientId is null or empty in emitPatientOrgListUpdate");
        }
    }
    // 字段名转换方法：将下划线命名转换为驼峰命名
    private void convertFieldNamesToCamelCase(JSONObject json) {
        // 处理 after 字段
        JSONObject after = json.getJSONObject("after");
        if (after != null) {
            JSONObject camelCaseAfter = new JSONObject();
            for (String key : after.keySet()) {
                String camelCaseKey = toCamelCase(key);
                camelCaseAfter.put(camelCaseKey, after.get(key));
            }
            json.put("after", camelCaseAfter);
        }

        // 处理 before 字段（用于删除操作）
        JSONObject before = json.getJSONObject("before");
        if (before != null) {
            JSONObject camelCaseBefore = new JSONObject();
            for (String key : before.keySet()) {
                String camelCaseKey = toCamelCase(key);
                camelCaseBefore.put(camelCaseKey, before.get(key));
            }
            json.put("before", camelCaseBefore);
        }
    }

    // 下划线转驼峰命名辅助方法
    private String toCamelCase(String underscoreName) {
        if (underscoreName == null || underscoreName.isEmpty()) {
            return underscoreName;
        }

        StringBuilder result = new StringBuilder();
        boolean nextUpperCase = false;

        for (int i = 0; i < underscoreName.length(); i++) {
            char currentChar = underscoreName.charAt(i);

            if (currentChar == '_') {
                nextUpperCase = true;
            } else {
                if (nextUpperCase) {
                    result.append(Character.toUpperCase(currentChar));
nextUpperCase = false;
                } else {
                    result.append(currentChar);
                }
            }
        }

        return result.toString();
    }

    // 遍历 JSON (主要是 after) 并格式化日期字段
    private void formatDatesInCamelCaseMap(JSONObject json) {
        JSONObject after = json.getJSONObject("after");
        if (after != null) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (String key : after.keySet()) {
                String lowerKey = key.toLowerCase();
                if (lowerKey.endsWith("time") || lowerKey.endsWith("date")) {
                    Object value = after.get(key);
                    if (value instanceof Number) {
                        try {
                            long ts = ((Number) value).longValue();
                            if (ts < 1000000000L) {
                                long millis = ts * 24L * 3600L * 1000L;
                                after.put(key, dateFormat.format(new Date(millis)));
                            } else {
                                after.put(key, dateFormat.format(new Date(ts)));
                            }
                        } catch (Exception e) {
                            // ignore conversion error
                        }
                    }
                }
            }
        }
    }

    private void enrichBuFollowUpPlan(JSONObject json) {
        JSONObject after = json.getJSONObject("after");
        if (after == null)
            return;

        String patientId = after.getString("patient_id");
        if (patientId != null) {
            try {
                String sql = "SELECT in_hospital_time, out_hospital_time FROM bu_bedside WHERE id = ?";
                try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setString(1, patientId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                            Timestamp inHospitalTime = rs.getTimestamp("in_hospital_time");
                            if (inHospitalTime != null) {
                                after.put("admissionDate", dateFormat.format(inHospitalTime));
                            } else {
                                after.put("admissionDate", null);
                            }

                            Date outHospitalTime = rs.getDate("out_hospital_time");
                            if (outHospitalTime != null) {
                                after.put("dischargeDate", dateFormat.format(outHospitalTime));
                            } else {
                                after.put("dischargeDate", null);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error enriching bu_follow_up_plan: " + e.getMessage());
            }
        }
    }

    private void enrichBuPatientOrg(JSONObject json) {
        JSONObject after = json.getJSONObject("after");
        if (after == null)
            return;

        String createOrg = after.getString("create_org");
        if (createOrg != null) {
            try {
                String sql = "SELECT name, status FROM sys_org WHERE id = ?";
                try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setString(1, createOrg);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            after.put("orgName", rs.getObject("name"));
                            int status = rs.getInt("status");
                            if (status != 0) {
                                after.put("useFlag", "0");
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error enriching bu_patient_org: " + e.getMessage());
            }
        }
    }
    // [修正] 检查某个年份是否还有其他档案存在
    private boolean checkIfYearHasOtherRecords(String patientId, String year, String excludeTaskId) {
        try {
            // [重要修正] 查询该患者在该年份下除了指定task_id之外的其他档案
            // 需要确保只查询有效的记录（非删除状态）
            String sql = "SELECT COUNT(*) as count FROM bu_oh_screening_interventions_info osii " +
                        "JOIN bu_task t ON osii.task_id = t.id " +
                        "JOIN bu_project p ON t.project_id = p.id " +
                        "WHERE osii.patient_id = ? AND p.year = ? AND osii.task_id != ? " +
                        "AND osii.useflag = 1 "; // [新增] 只查询有效记录
            
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, patientId);
                stmt.setString(2, year);
                stmt.setString(3, excludeTaskId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int count = rs.getInt("count");
                        System.out.println("Checking other records for patient " + patientId + 
                                         ", year " + year + ", exclude task " + excludeTaskId +
                                         ": found " + count + " other records");
                        return count > 0;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error checking if year has other records for patient " + patientId + ": " + e.getMessage());
        }
        return false; // 如果查询失败，默认返回false（没有其他记录）
    }

    // [修正] 处理建档时间更新的方法 - 只处理更新，不发送原始记录
    private void handlePatientRecordTimeUpdate(JSONObject json, Collector<JSONObject> out) {
        JSONObject after = json.getJSONObject("after");
        JSONObject before = json.getJSONObject("before");
        String op = json.getString("op");
        
        // [修正] 字段名转换之前，应该使用原始下划线字段名
        String patientId = null;
        if (after != null) {
            patientId = after.getString("patient_id");
        } else if (before != null) {
            patientId = before.getString("patient_id");
        }
        
        if (patientId == null || patientId.isEmpty()) {
            return; // 跳过处理
        }

        try {
            // [修正] 获取操作类型对应的task_id（下划线命名）
            String taskId = null;
            if ("c".equals(op) || "u".equals(op) || "r".equals(op)) {
                if (after != null) {
                    taskId = after.getString("task_id");
                }
            } else if ("d".equals(op)) {
                if (before != null) {
                    taskId = before.getString("task_id");
                }
            }

            if (taskId == null || taskId.isEmpty()) {
                return; // 跳过处理
            }

            // 通过task_id查询project_id和年份
            String year = null;
            String sql = "SELECT p.year FROM bu_task t JOIN bu_project p ON t.project_id = p.id WHERE t.id = ?";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, taskId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        year = rs.getString("year");
                    }
                }
            }

            if (year == null || year.isEmpty()) {
                return; // 跳过处理
            }

            // [修改] 从Redis获取当前患者数据
            JSONObject currentPatientData = getCurrentPatientData(patientId);
            if (currentPatientData == null || currentPatientData.isEmpty()) {
                return; // 如果患者数据为空，跳过处理
            }

            // [修改] 从当前数据中获取现有的recordTime数组
            List<String> recordTimeList = new ArrayList<>();
            Object existingRecordTime = currentPatientData.get("recordTime");
            if (existingRecordTime instanceof List) {
                recordTimeList = (List<String>) existingRecordTime;
            } else if (existingRecordTime instanceof String) {
                String[] existingTimes = ((String) existingRecordTime).split(",");
                for (String time : existingTimes) {
                    if (!time.trim().isEmpty()) {
                        recordTimeList.add(time.trim());
                    }
                }
            }

            // 根据操作类型处理recordTime数组
            boolean needUpdate = false;
            
            if ("c".equals(op) || "u".equals(op) || "r".equals(op)) {
                // 添加年份（如果不存在）
                if (!recordTimeList.contains(year)) {
                    recordTimeList.add(year);
                    needUpdate = true;
                    System.out.println("Adding year " + year + " to recordTime for patient " + patientId);
                }
            } else if ("d".equals(op)) {
                // [重要修正] 删除操作：检查是否可以删除年份
                if (recordTimeList.contains(year)) {
                    boolean hasOtherRecords = checkIfYearHasOtherRecords(patientId, year, taskId);
                    if (!hasOtherRecords) {
                        // [重要修正] 只删除特定的年份，而不是整个列表
                        // 使用新的列表来避免并发问题
                        List<String> newRecordTimeList = new ArrayList<>(recordTimeList);
                        newRecordTimeList.remove(year);
                        recordTimeList = newRecordTimeList;
                        needUpdate = true;
                        System.out.println("Removing year " + year + " from recordTime for patient " + patientId + 
                                         ". New recordTime: " + recordTimeList);
                    } else {
                        System.out.println("Cannot remove year " + year + " from recordTime for patient " + patientId + 
                                         " because other records exist");
                    }
                }
            }

            // [修正] 如果需要更新，构造更新消息并更新Redis缓存
            if (needUpdate) {
                JSONObject updateDoc = new JSONObject();
                updateDoc.put("_meili_index", indexPatient);

                // [修改] 使用当前患者数据，而不是原始数据库数据
                JSONObject content = new JSONObject(currentPatientData);

                // [修正] 新增或更新recordTime字段，确保使用正确的字符串列表格式
                content.put("id", patientId);
                
                // [修正] 确保recordTime是字符串列表，而不是对象列表
                List<String> stringRecordTimeList = new ArrayList<>();
                for (String yearStr : recordTimeList) {
                    stringRecordTimeList.add(yearStr); // 确保每个元素都是字符串
                }
                content.put("recordTime", stringRecordTimeList);

                // [修正] 将content放入after字段，并调用字段名转换方法
                updateDoc.put("after", content);
                convertFieldNamesToCamelCase(updateDoc);

                // 构造 update 类型的 CDC 结构
                updateDoc.put("op", "u");

                // 发送到Meilisearch
                out.collect(updateDoc);

                // [新增] 更新Redis缓存
                updateRedisCache(patientId, content);
                
                System.out.println("RecordTime updated for patient " + patientId + ": " + stringRecordTimeList);
            } else {
                System.out.println("No update needed for patient " + patientId + ". Current recordTime: " + recordTimeList);
            }

        } catch (Exception e) {
            System.err.println("Error handling patient record time update: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // [删除重复的方法定义] 保留第425-450行的checkIfYearHasOtherRecords方法
    // 删除第589-614行的重复方法定义

    // [新增] 更新Redis缓存的方法
    private void updateRedisCache(String patientId, JSONObject patientData) {
        try {
            String redisKey = "patient:" + patientId;
            // 设置过期时间为1小时，避免缓存无限增长
            jedis.setex(redisKey, 3600, patientData.toJSONString());
        } catch (Exception e) {
            System.err.println("Error updating Redis cache for patient " + patientId + ": " + e.getMessage());
        }
    }

    /**
     * 处理sys_user表的同步逻辑
     * 删除标识：status为2表示删除
     * 同步SQL：select u.id, u.account, u.nick_name, u.name, u.avatar, u.birthday, u.sex, u.email, u.phone, u.tel, u.admin_type, u.status, u.create_time, u.create_user, u.id_card, u.audit_status, u.audit_reason, e.org_id from sys_user u 
     * left join sys_emp e on u.id = e.id 
     * where u.`status` != 2
     */
    private void enrichSysUser(JSONObject json) {
        JSONObject after = json.getJSONObject("after");
        JSONObject before = json.getJSONObject("before");
        String op = json.getString("op");
        
        // 如果是删除操作，检查删除标识
        if ("d".equals(op) && before != null) {
            Integer status = before.getInteger("status");
            if (status != null && status == 2) {
                // 如果是删除标识为2的记录，不进行同步
                json.put("_skip_sync", true);
                return;
            }
        }
        
        // 如果是插入或更新操作，检查删除标识
        if (("c".equals(op) || "u".equals(op)) && after != null) {
            Integer status = after.getInteger("status");
            if (status != null && status == 2) {
                // 如果是删除标识为2的记录，不进行同步
                json.put("_skip_sync", true);
                return;
            }
        }
        
        // 对于需要同步的记录，执行关联查询获取完整数据
        if (after != null) {
            String userId = after.getString("id");
            if (userId != null) {
                try {
                    String sql = "SELECT u.id, u.account, u.nick_name, u.name, u.avatar, u.birthday, u.sex, u.email, u.phone, u.tel, u.admin_type, u.status, u.create_time, u.create_user, u.id_card, u.audit_status, u.audit_reason, e.org_id " +
                                 "FROM sys_user u " +
                                 "LEFT JOIN sys_emp e ON u.id = e.id " +
                                 "WHERE u.id = ? AND u.status != 2";
                    
                    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                        stmt.setString(1, userId);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                // 清空原有数据，使用查询结果替换
                                after.clear();
                                
                                // 添加所有查询字段到after对象
                                after.put("id", rs.getString("id"));
                                after.put("account", rs.getString("account"));
                                after.put("nickName", rs.getString("nick_name"));
                                after.put("name", rs.getString("name"));
                                after.put("avatar", rs.getString("avatar"));
                                after.put("birthday", rs.getDate("birthday"));
                                after.put("sex", rs.getString("sex"));
                                after.put("email", rs.getString("email"));
                                after.put("phone", rs.getString("phone"));
                                after.put("tel", rs.getString("tel"));
                                after.put("adminType", rs.getString("admin_type"));
                                after.put("status", rs.getInt("status"));
                                after.put("createTime", rs.getTimestamp("create_time"));
                                after.put("createUser", rs.getString("create_user"));
                                after.put("idCard", rs.getString("id_card"));
                                after.put("auditStatus", rs.getString("audit_status"));
                                after.put("auditReason", rs.getString("audit_reason"));
                                after.put("orgId", rs.getString("org_id"));
                            } else {
                                // 如果查询不到记录，说明记录已被删除，跳过同步
                                json.put("_skip_sync", true);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error enriching sys_user data for user " + userId + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * 处理sys_org表的同步逻辑
     * 删除标识：useflag为1表示删除
     * 同步SQL：select o.id, o.pid, o.pids, o.name, o.code, o.sort, o.remark, o.status, o.useflag, o.create_time, o.create_user, o.org_type, e.audit_status, e.audit_time, e.audit_refuse_reason, e.unit_type, e.province, e.city, e.district, e.abbreviation, e.register_type, e.level_value, e.level_name, e.category_value, e.category_name, e.practicing_license, e.social_credit_code, e.mobile, e.address, e.manager_name, e.manager_idcard, e.manager_sex, e.manager_phone, e.manager_mail, e.cert, e.useflag, e.create_time, e.create_user, e.update_time, e.update_user, e.post_code, e.org_introduce, e.social_credit_cert, e.org_website, e.org_in_area, e.is_Study, e.is_Pro, e.org_lat_lon 
     * from sys_org o 
     * left join sys_org_expand e on o.id = e.org_id 
     * where o.useflag = 1
     */
    private void enrichSysOrg(JSONObject json) {
        JSONObject after = json.getJSONObject("after");
        JSONObject before = json.getJSONObject("before");
        String op = json.getString("op");
        
        // 如果是删除操作，检查删除标识
        if ("d".equals(op) && before != null) {
            Integer useflag = before.getInteger("useflag");
            if (useflag != null && useflag == 1) {
                // 如果是删除标识为1的记录，不进行同步
                json.put("_skip_sync", true);
                return;
            }
        }
        
        // 如果是插入或更新操作，检查删除标识
        if (("c".equals(op) || "u".equals(op)) && after != null) {
            Integer useflag = after.getInteger("useflag");
            if (useflag != null && useflag == 1) {
                // 如果是删除标识为1的记录，不进行同步
                json.put("_skip_sync", true);
                return;
            }
        }
        
        // 对于需要同步的记录，执行关联查询获取完整数据
        if (after != null) {
            String orgId = after.getString("id");
            if (orgId != null) {
                try {
                    String sql = "SELECT o.id, o.pid, o.pids, o.name, o.code, o.sort, o.remark, o.status, o.useflag, o.create_time, o.create_user, o.org_type, " +
                                 "e.audit_status, e.audit_time, e.audit_refuse_reason, e.unit_type, e.province, e.city, e.district, e.abbreviation, e.register_type, " +
                                 "e.level_value, e.level_name, e.category_value, e.category_name, e.practicing_license, e.social_credit_code, e.mobile, e.address, " +
                                 "e.manager_name, e.manager_idcard, e.manager_sex, e.manager_phone, e.manager_mail, e.cert, e.useflag as expand_useflag, " +
                                 "e.create_time as expand_create_time, e.create_user as expand_create_user, e.update_time, e.update_user, e.post_code, e.org_introduce, " +
                                 "e.social_credit_cert, e.org_website, e.org_in_area, e.is_Study, e.is_Pro, e.org_lat_lon " +
                                 "FROM sys_org o " +
                                 "LEFT JOIN sys_org_expand e ON o.id = e.org_id " +
                                 "WHERE o.id = ? AND o.useflag = 1";
                    
                    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                        stmt.setString(1, orgId);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                // 清空原有数据，使用查询结果替换
                                after.clear();
                                
                                // 添加sys_org表字段
                                after.put("id", rs.getString("id"));
                                after.put("pid", rs.getString("pid"));
                                after.put("pids", rs.getString("pids"));
                                after.put("name", rs.getString("name"));
                                after.put("code", rs.getString("code"));
                                after.put("sort", rs.getInt("sort"));
                                after.put("remark", rs.getString("remark"));
                                after.put("status", rs.getString("status"));
                                after.put("useflag", rs.getInt("useflag"));
                                after.put("createTime", rs.getTimestamp("create_time"));
                                after.put("createUser", rs.getString("create_user"));
                                after.put("orgType", rs.getString("org_type"));
                                
                                // 添加sys_org_expand表字段
                                after.put("auditStatus", rs.getString("audit_status"));
                                after.put("auditTime", rs.getTimestamp("audit_time"));
                                after.put("auditRefuseReason", rs.getString("audit_refuse_reason"));
                                after.put("unitType", rs.getString("unit_type"));
                                after.put("province", rs.getString("province"));
                                after.put("city", rs.getString("city"));
                                after.put("district", rs.getString("district"));
                                after.put("abbreviation", rs.getString("abbreviation"));
                                after.put("registerType", rs.getString("register_type"));
                                after.put("levelValue", rs.getString("level_value"));
                                after.put("levelName", rs.getString("level_name"));
                                after.put("categoryValue", rs.getString("category_value"));
                                after.put("categoryName", rs.getString("category_name"));
                                after.put("practicingLicense", rs.getString("practicing_license"));
                                after.put("socialCreditCode", rs.getString("social_credit_code"));
                                after.put("mobile", rs.getString("mobile"));
                                after.put("address", rs.getString("address"));
                                after.put("managerName", rs.getString("manager_name"));
                                after.put("managerIdcard", rs.getString("manager_idcard"));
                                after.put("managerSex", rs.getString("manager_sex"));
                                after.put("managerPhone", rs.getString("manager_phone"));
                                after.put("managerMail", rs.getString("manager_mail"));
                                after.put("cert", rs.getString("cert"));
                                after.put("expandUseflag", rs.getInt("expand_useflag"));
                                after.put("expandCreateTime", rs.getTimestamp("expand_create_time"));
                                after.put("expandCreateUser", rs.getString("expand_create_user"));
                                after.put("updateTime", rs.getTimestamp("update_time"));
                                after.put("updateUser", rs.getString("update_user"));
                                after.put("postCode", rs.getString("post_code"));
                                after.put("orgIntroduce", rs.getString("org_introduce"));
                                after.put("socialCreditCert", rs.getString("social_credit_cert"));
                                after.put("orgWebsite", rs.getString("org_website"));
                                after.put("orgInArea", rs.getString("org_in_area"));
                                after.put("isStudy", rs.getString("is_Study"));
                                after.put("isPro", rs.getString("is_Pro"));
                                after.put("orgLatLon", rs.getString("org_lat_lon"));
                            } else {
                                // 如果查询不到记录，说明记录已被删除，跳过同步
                                json.put("_skip_sync", true);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error enriching sys_org data for org " + orgId + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * 在发送记录前检查是否需要跳过同步
     */
    private boolean shouldSkipSync(JSONObject json) {
        return json.getBooleanValue("_skip_sync");
    }
}