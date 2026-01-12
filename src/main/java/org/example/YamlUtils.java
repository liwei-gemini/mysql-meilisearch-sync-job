package org.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class YamlUtils {

    /**
     * 加载 YAML 文件并转换为 Flink 的 ParameterTool
     */
    public static ParameterTool loadYaml(String fileName) {
        try (InputStream inputStream = YamlUtils.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream == null) {
                System.err.println("Warning: " + fileName + " not found in classpath.");
                return ParameterTool.fromMap(new HashMap<>());
            }

            Yaml yaml = new Yaml();
            Map<String, Object> yamlMap = yaml.load(inputStream);

            // 将嵌套的 Map 扁平化 (e.g., mysql.hostname)
            Map<String, String> resultMap = new HashMap<>();
            flatten("", yamlMap, resultMap);

            return ParameterTool.fromMap(resultMap);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load yaml file", e);
        }
    }

    /**
     * 递归将嵌套 Map 转换为点分隔的 Key
     */
    @SuppressWarnings("unchecked")
    private static void flatten(String prefix, Map<String, Object> map, Map<String, String> result) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                // 递归处理嵌套 Map
                flatten(key, (Map<String, Object>) value, result);
            } else {
                // 转换为 String 存入结果
                result.put(key, value == null ? "" : value.toString());
            }
        }
    }
}
