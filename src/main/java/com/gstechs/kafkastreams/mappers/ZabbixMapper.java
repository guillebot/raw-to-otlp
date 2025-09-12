package com.gstechs.kafkastreams.mappers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZabbixMapper implements OtlpJsonMapper {
    private static final ObjectMapper M = new ObjectMapper();

    // Patterns to extract embedded labels from Zabbix metric names
    private static final Pattern LEADING_PORT = Pattern.compile("^(\d{1,5})\s+(.+)$");

    @Override
    public String toOtlpJson(String zabbixJson, String inputTopic) throws Exception {
        JsonNode root = M.readTree(zabbixJson);
        long clockSec = root.path("clock").asLong(System.currentTimeMillis()/1000L);
        long ns = root.path("ns").asLong(0L);
        long timeUnixNano = clockSec * 1_000_000_000L + ns;

        // --- Extract base name + embedded labels from Zabbix "name" ---
        String rawName = root.path("name").asText("zabbix.metric");
        String baseName = rawName; // will be trimmed of embedded labels
        String portLabel = null;
        String hostLabel = null;

        // 1) Trailing host separator variants: " - ", "_-_" (observed in examples)
        int sepIdx = lastIndexOfAny(baseName, " - ", "_-_");
        if (sepIdx >= 0) {
            String candidate = baseName.substring(sepIdx + (baseName.startsWith("_-_", sepIdx) ? 3 : 3)).trim();
            // Accept as host if looks like host: has dot or colon (e.g., fqdn or host:port)
            if (candidate.contains(".") || candidate.contains(":")) {
                hostLabel = candidate;
                baseName = baseName.substring(0, sepIdx).trim();
            }
        }

        // 2) Leading port number pattern: "443 Port State" → port=443, name="Port State"
        Matcher m = LEADING_PORT.matcher(baseName);
        if (m.matches()) {
            portLabel = m.group(1);
            baseName = m.group(2).trim();
        }

        // Normalize only spaces → underscores (per your rule)
        String metricName = baseName.replace(' ', '_');

        // --- value ---
        double metricValue;
        JsonNode vNode = root.path("value");
        if (vNode.isNumber()) {
            metricValue = vNode.asDouble();
        } else {
            try { metricValue = Double.parseDouble(vNode.asText("0")); } catch (Exception e) { metricValue = 0.0; }
        }

        // --- resource attrs ---
        String hostName = root.path("host").isObject()
                ? root.path("host").path("name").asText(root.path("host").path("host").asText(null))
                : root.path("host").asText(null);

        ObjectNode out = M.createObjectNode();
        ObjectNode resource = M.createObjectNode();
        ArrayNode rAttrs = M.createArrayNode();
        if (hostName != null && !hostName.isEmpty()) rAttrs.add(attr("host.name", hostName));
        rAttrs.add(attr("source", "zabbix"));
        rAttrs.add(attr("kafka.topic", inputTopic));
        if (root.path("groups").isArray()) {
            for (JsonNode g : root.path("groups")) {
                rAttrs.add(attr("zabbix.group", g.asText()));
            }
        }
        if (root.has("itemid")) rAttrs.add(attr("zabbix.itemid", root.get("itemid").asText()));
        if (root.has("type"))   rAttrs.add(attr("zabbix.type", root.get("type").asText()));
        resource.set("attributes", rAttrs);

        // --- metric ---
        ObjectNode resourceMetric = M.createObjectNode();
        resourceMetric.set("resource", resource);

        ObjectNode scopeMetric = M.createObjectNode();
        ObjectNode scope = M.createObjectNode();
        scope.put("name", "kafka");
        scope.put("version", "streams");
        scopeMetric.set("scope", scope);

        ArrayNode metrics = M.createArrayNode();
        ObjectNode metric = M.createObjectNode();
        metric.put("name", metricName);

        ObjectNode gauge = M.createObjectNode();
        ArrayNode dps = M.createArrayNode();
        ObjectNode dp = M.createObjectNode();
        dp.put("asDouble", metricValue);
        dp.put("timeUnixNano", timeUnixNano);

        ArrayNode pAttrs = M.createArrayNode();
        if (hostName != null && !hostName.isEmpty()) pAttrs.add(attr("host.name", hostName));
        if (portLabel != null) pAttrs.add(attr("port", portLabel));
        if (hostLabel != null) pAttrs.add(attr("host", hostLabel));
        if (root.path("item_tags").isArray()) {
            for (JsonNode t : root.path("item_tags")) {
                String k = t.path("tag").asText(null);
                String v = t.path("value").asText(null);
                if (k != null && v != null) pAttrs.add(attr("zbx.tag." + k, v));
            }
        }
        if (pAttrs.size() > 0) dp.set("attributes", pAttrs);

        dps.add(dp);
        gauge.set("dataPoints", dps);
        metric.set("gauge", gauge);
        metrics.add(metric);

        scopeMetric.set("metrics", metrics);
        ArrayNode scopeMetrics = M.createArrayNode();
        scopeMetrics.add(scopeMetric);
        resourceMetric.set("scopeMetrics", scopeMetrics);

        ArrayNode resourceMetrics = M.createArrayNode();
        resourceMetrics.add(resourceMetric);
        out.set("resourceMetrics", resourceMetrics);

        return M.writeValueAsString(out);
    }

    private static int lastIndexOfAny(String s, String... seps) {
        int best = -1;
        for (String sep : seps) {
            int i = s.lastIndexOf(sep);
            if (i > best) best = i;
        }
        return best;
    }

    private ObjectNode attr(String key, String value) {
        ObjectNode attr = M.createObjectNode();
        attr.put("key", key);
        ObjectNode val = M.createObjectNode();
        val.put("stringValue", value);
        attr.set("value", val);
        return attr;
    }
}