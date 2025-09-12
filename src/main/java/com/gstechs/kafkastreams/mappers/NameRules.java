// =============================
// File: src/main/java/com/gstechs/kafkastreams/mappers/NameRules.java
// =============================
package com.gstechs.kafkastreams.mappers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data-driven regex engine for mapping Zabbix names to base + attributes.
 * Rules are applied in order; the first match wins.
 */
public final class NameRules {
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("zabbix.rules.debug", "true"));
    private static final boolean DEBUG_APPLY = Boolean.parseBoolean(System.getProperty("zabbix.rules.debug.apply", "false"));

    public record AttrSpec(String name, String from_group) {}
    public record RuleSpec(String id, String pattern, List<AttrSpec> attributes) {}
    public record Parsed(String base, Map<String,String> attributes) {}

    private final List<NameRule> rules;

    private record NameRule(String id, Pattern pattern, List<AttrSpec> attrs) {}

    private NameRules(List<NameRule> rules) {
        this.rules = rules;
    }

    public int ruleCount() { return rules.size(); }

    public static NameRules load(String filePathOrNull, String classpathFallback) {
        // Try explicit file path first
        if (filePathOrNull != null && !filePathOrNull.isBlank()) {
            try {
                if (DEBUG) System.out.println("NameRules: loading from file: " + filePathOrNull);
                return fromYaml(Files.readString(Path.of(filePathOrNull)), "file:" + filePathOrNull);
            } catch (Exception e) {
                System.err.println("NameRules: failed to load from file: " + filePathOrNull);
                e.printStackTrace();
            }
        }
        // Fallback to classpath
        try (InputStream in = NameRules.class.getResourceAsStream(classpathFallback)) {
            if (in != null) {
                if (DEBUG) System.out.println("NameRules: loading from classpath: " + classpathFallback);
                return fromYaml(new String(in.readAllBytes()), "classpath:" + classpathFallback);
            } else {
                System.err.println("NameRules: classpath resource not found: " + classpathFallback);
            }
        } catch (IOException e) {
            System.err.println("NameRules: error reading classpath resource: " + classpathFallback);
            e.printStackTrace();
        }
        // Empty ruleset as last resort
        if (DEBUG) System.out.println("NameRules: loaded 0 rules (EMPTY)");
        return new NameRules(Collections.emptyList());
    }

    private static NameRules fromYaml(String yaml, String origin) throws IOException {
        JsonNode root = YAML.readTree(yaml);
        if (root == null || root.get("rules") == null || !root.get("rules").isArray()) {
            System.err.println("NameRules: YAML has no 'rules' array (origin=" + origin + ")");
            return new NameRules(Collections.emptyList());
        }
        List<RuleSpec> specs = JSON.convertValue(root.get("rules"), new TypeReference<>(){});
        List<NameRule> compiled = new ArrayList<>();
        for (RuleSpec rs : specs) {
            Pattern p = Pattern.compile(rs.pattern());
            compiled.add(new NameRule(
                    rs.id(),
                    p,
                    rs.attributes() == null ? List.of() : rs.attributes()
            ));
        }
        if (DEBUG) {
            System.out.println("NameRules: loaded " + compiled.size() + " rules from " + origin);
            for (int i = 0; i < compiled.size(); i++) {
                System.out.println("  [" + i + "] id=" + compiled.get(i).id() + ", pattern=" + compiled.get(i).pattern());
            }
        }
        return new NameRules(Collections.unmodifiableList(compiled));
    }

    public Parsed apply(String rawName) {
        if (rawName == null) return new Parsed(null, Map.of());
        for (NameRule r : rules) {
            Matcher m = r.pattern.matcher(rawName);
            if (m.matches()) {
                String base = groupSafely(m, "base");
                Map<String,String> attrs = new LinkedHashMap<>();
                if (r.attrs != null) {
                    for (AttrSpec a : r.attrs) {
                        String val = groupSafely(m, a.from_group());
                        if (val != null) attrs.put(a.name(), val);
                    }
                }
                if (DEBUG_APPLY) {
                    System.out.println("NameRules.apply: matched rule id=" + r.id() + ", base=" + base + ", attrs=" + attrs + ", raw='" + rawName + "'");
                }
                return new Parsed(base, attrs);
            }
        }
        if (DEBUG_APPLY) {
            System.out.println("NameRules.apply: no match, raw='" + rawName + "'");
        }
        return new Parsed(null, Map.of());
    }

    private static String groupSafely(Matcher m, String name) {
        try { return m.group(name); } catch (Exception e) { return null; }
    }
}

