package com.gstechs.kafkastreams.mappers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ZabbixMapper implements OtlpJsonMapper {
    private static final ObjectMapper M = new ObjectMapper();

    @Override
    public String toOtlpJson(String zabbixJson, String inputTopic) throws Exception {
        JsonNode root = M.readTree(zabbixJson);
        long clockSec = root.path("clock").asLong(System.currentTimeMillis()/1000L);
        long ns = root.path("ns").asLong(0L);
        long timeUnixNano = clockSec * 1_000_000_000L + ns;

        String metricName = root.path("name").asText("zabbix.metric");
        double metricValue = root.path("value").isNumber() ? root.path("value").asDouble() : 0.0;

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

    private ObjectNode attr(String key, String value) {
        ObjectNode attr = M.createObjectNode();
        attr.put("key", key);
        ObjectNode val = M.createObjectNode();
        val.put("stringValue", value);
        attr.set("value", val);
        return attr;
    }
}
