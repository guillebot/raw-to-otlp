package com.gstechs.kafkastreams.mappers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Iterator;
import java.util.Map;

/**
 * Netscout → OTLP JSON mapper.
 * Converts a rich Netscout record into multiple OTLP Gauge metrics under one Resource.
 */
public class NetscoutMapper implements OtlpJsonMapper {
    private static final ObjectMapper M = new ObjectMapper();

    @Override
    public String toOtlpJson(String netscoutJson, String inputTopic) throws Exception {
        JsonNode root = M.readTree(netscoutJson);

        // 1) Timestamp → nanoseconds (e.g., "2025-09-09 18:05:00.000000 UTC")
        long timeUnixNano = parseCalTsToNanos(root.path("cal_timestamp_time").asText(null));
        if (timeUnixNano <= 0) {
            timeUnixNano = System.currentTimeMillis() * 1_000_000L;
        }

        // 2) Resource attributes (device, app, site, vlan, etc.)
        ObjectNode resource = M.createObjectNode();
        ArrayNode rAttrs = M.createArrayNode();
        putAttrIfNonEmpty(rAttrs, "source", "netscout");
        putAttrIfNonEmpty(rAttrs, "kafka.topic", inputTopic);
        putAttrIfNonEmpty(rAttrs, "device.name", text(root, "device_name"));
        putAttrIfNonEmpty(rAttrs, "vlan.name", text(root, "vlan_name"));
        putAttrIfNonEmpty(rAttrs, "client.site", text(root, "client_site_name"));
        putAttrIfNonEmpty(rAttrs, "application.name", text(root, "application_name"));
        putAttrIfNonEmpty(rAttrs, "application.group", text(root, "application_group"));
        putAttrIfNonEmpty(rAttrs, "app.protocol.type", text(root, "application_protocol_type_code"));
        resource.set("attributes", rAttrs);

        // 3) Metrics: iterate all fields with prefix upw_ and numeric values
        ArrayNode metrics = M.createArrayNode();
        for (Iterator<Map.Entry<String, JsonNode>> it = root.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String name = e.getKey();
            JsonNode val = e.getValue();
            if (!name.startsWith("upw_")) continue;
            if (!val.isNumber()) continue;

            ObjectNode metric = M.createObjectNode();
            metric.put("name", "netscout." + name);
            String unit = inferUnit(name);
            if (!unit.isEmpty()) metric.put("unit", unit);

            ObjectNode gauge = M.createObjectNode();
            ArrayNode dps = M.createArrayNode();
            ObjectNode dp = M.createObjectNode();
            dp.put("asDouble", val.asDouble());
            dp.put("timeUnixNano", timeUnixNano);
            // (Optional) per-point attributes: echo application/host hints
            ArrayNode pAttrs = M.createArrayNode();
            putAttrIfNonEmpty(pAttrs, "device.name", text(root, "device_name"));
            putAttrIfNonEmpty(pAttrs, "client.site", text(root, "client_site_name"));
            if (pAttrs.size() > 0) dp.set("attributes", pAttrs);

            dps.add(dp);
            gauge.set("dataPoints", dps);
            metric.set("gauge", gauge);
            metrics.add(metric);
        }

        // 4) Wrap into scope/resource/otlp envelope
        ObjectNode scopeMetric = M.createObjectNode();
        scopeMetric.putObject("scope").put("name", "kafka").put("version", "streams");
        scopeMetric.set("metrics", metrics);

        ArrayNode scopeMetrics = M.createArrayNode();
        scopeMetrics.add(scopeMetric);

        ObjectNode resourceMetric = M.createObjectNode();
        resourceMetric.set("resource", resource);
        resourceMetric.set("scopeMetrics", scopeMetrics);

        ArrayNode resourceMetrics = M.createArrayNode();
        resourceMetrics.add(resourceMetric);

        ObjectNode out = M.createObjectNode();
        out.set("resourceMetrics", resourceMetrics);
        return M.writeValueAsString(out);
    }

    private static long parseCalTsToNanos(String ts) {
        if (ts == null || ts.isBlank()) return -1L;
        try {
            DateTimeFormatter f = new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .appendLiteral('.')
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 1, 6, false)
                    .appendLiteral(' ')
                    .appendLiteral("UTC")
                    .toFormatter();
            LocalDateTime ldt = LocalDateTime.parse(ts, f);
            Instant inst = ldt.toInstant(ZoneOffset.UTC);
            long base = inst.getEpochSecond() * 1_000_000_000L;
            long nanos = ldt.getNano(); // includes micros → nanos
            return base + nanos;
        } catch (Exception ignore) {
            return -1L;
        }
    }

    private static String inferUnit(String name) {
        if (name.endsWith("_bytes_count") || name.contains("bytes")) return "bytes";
        if (name.endsWith("_packets_count")) return "packets";
        if (name.endsWith("_kbps")) return "kbps";
        if (name.endsWith("_millis")) return "ms";
        if (name.endsWith("_usec") || name.contains("_rtt_")) return "us";
        if (name.endsWith("_count")) return "count";
        return "";
    }

    private static void putAttrIfNonEmpty(ArrayNode attrs, String key, String value) {
        if (value == null) return;
        String v = value.trim();
        if (v.isEmpty()) return;
        attrs.add(attr(key, v));
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.path(field);
        return v.isNull() ? null : v.asText(null);
    }

    private static ObjectNode attr(String key, String value) {
        ObjectNode attr = M.createObjectNode();
        attr.put("key", key);
        ObjectNode val = M.createObjectNode();
        val.put("stringValue", value);
        attr.set("value", val);
        return attr;
    }
}