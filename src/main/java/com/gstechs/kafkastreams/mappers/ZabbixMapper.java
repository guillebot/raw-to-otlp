package com.gstechs.kafkastreams.mappers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZabbixMapper implements OtlpJsonMapper {
    private static final ObjectMapper M = new ObjectMapper();

    // ===== Regex "cases" for Zabbix name parsing =====
    // Case A: leading port + specific text → "443 Port State" → port=443, base="Port State"
    private static final Pattern CASE_LEADING_PORT = Pattern.compile("^(\\d{1,5})\\s+(Port State.*)$");
    // Case B: trailing host after separators → "... - host:21" or "..._-_host:21"
    private static final Pattern CASE_TRAILING_HOST = Pattern.compile("^(.*?)(?:\\s-\\s|_-_)([^\\s].*)$");
    // Case C: cert expiration form → "Cert Expiration Date: hostname" → base="Cert Expiration Date", host=hostname
    private static final Pattern CASE_CERT_HOST = Pattern.compile("^(Cert Expiration Date):\\s+(.+)$");
    // Case D: disk utilization → explicitly match "Disk Utilization on /path"
    private static final Pattern CASE_DISK = Pattern.compile("^(Disk Utilization) on (/.+)$");
    // Case E: used disk space → explicitly match "Used disk space on /path"
    private static final Pattern CASE_USED_DISK = Pattern.compile("^(Used disk space) on (/.+)$");
    // Case F: storage metrics with path → "/path: Storage ..."
    private static final Pattern CASE_STORAGE = Pattern.compile("^(/.+?):\\s+(Storage.*)$");
    // Case G: total/used space with path, including root path "/" → "/path: Total space..." or "/: Used space..."
    private static final Pattern CASE_SPACE = Pattern.compile("^(/.*?):\\s+((Total|Used) space.*)$");
    // Case H: CPU utilization with id → "#773: CPU utilization" → base="CPU utilization", cpu=773 (allow 1+ digits)
    private static final Pattern CASE_CPU = Pattern.compile("^#([0-9]+):\\s+(CPU utilization.*)$");
    // Case I: storage metrics like "/ddr/ext: Storage units" or "Storage utilization"
    private static final Pattern CASE_STORAGE_UNITS = Pattern.compile("^(/.+?):\\s+(Storage (units|utilization))$");

    @Override
    public String toOtlpJson(String zabbixJson, String inputTopic) throws Exception {
        JsonNode root = M.readTree(zabbixJson);
        // Ignore messages with type not 0 or 3
        int type = root.path("type").asInt(-1);
        if (!(type == 0 || type == 3)) {
            return null; // signal to caller to drop/ignore
        }

        long clockSec = root.path("clock").asLong(System.currentTimeMillis()/1000L);
        long ns = root.path("ns").asLong(0L);
        long timeUnixNano = clockSec * 1_000_000_000L + ns;

        // === Parse name via ordered regex cases ===
        String rawName = root.path("name").asText("zabbix.metric");
        NameParts parts = parseNameCases(rawName);
        String metricName = parts.base.replace(' ', '_'); // normalize only spaces

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
        if (parts.port != null) pAttrs.add(attr("port", parts.port));
        if (parts.host != null) pAttrs.add(attr("host", parts.host));
        if (parts.disk != null) pAttrs.add(attr("disk", parts.disk));
        if (parts.cpu != null) pAttrs.add(attr("cpu", parts.cpu));
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

    // Ordered application of regex cases; first match wins for each kind of extraction.
    private static NameParts parseNameCases(String raw) {
        NameParts p = new NameParts();
        p.base = raw == null ? "zabbix.metric" : raw.trim();

        // Case B: trailing host (" - host" or "_-_host")
        Matcher tb = CASE_TRAILING_HOST.matcher(p.base);
        if (tb.matches()) {
            String left = tb.group(1).trim();
            String right = tb.group(2).trim();
            if (right.contains(".") || right.contains(":")) {
                p.host = right;
                p.base = left;
            }
        }

        // Case C: "Cert Expiration Date: host" → host=..., base="Cert Expiration Date"
        Matcher mc = CASE_CERT_HOST.matcher(p.base);
        if (mc.matches()) {
            p.base = mc.group(1).trim();
            p.host = mc.group(2).trim();
        }

        // Case A: leading port + specific text "Port State"
        Matcher ma = CASE_LEADING_PORT.matcher(p.base);
        if (ma.matches()) {
            p.port = ma.group(1);
            p.base = ma.group(2).trim();
        }

        // Case D: disk utilization → explicitly match "Disk Utilization on /path"
        Matcher md = CASE_DISK.matcher(p.base);
        if (md.matches()) {
            p.base = md.group(1).trim();
            p.disk = md.group(2).trim();
        }

        // Case E: used disk space → explicitly match "Used disk space on /path"
        Matcher me = CASE_USED_DISK.matcher(p.base);
        if (me.matches()) {
            p.base = me.group(1).trim();
            p.disk = me.group(2).trim();
        }

        // Case F: storage metrics with path → "/path: Storage ..."
        Matcher mf = CASE_STORAGE.matcher(p.base);
        if (mf.matches()) {
            p.disk = mf.group(1).trim();
            p.base = mf.group(2).trim();
        }

        // Case G: total/used space with path → "/path: Total space..." or "/path: Used space..." (allow root "/")
        Matcher mg = CASE_SPACE.matcher(p.base);
        if (mg.matches()) {
            p.disk = mg.group(1).trim();
            p.base = mg.group(2).trim();
        }

        // Case H: CPU utilization with id → "#773: CPU utilization"
        Matcher mh = CASE_CPU.matcher(p.base);
        if (mh.matches()) {
            p.cpu = mh.group(1).trim();
            p.base = mh.group(2).trim();
        }

        // Case I: storage units/utilization with path → "/path: Storage units" or "Storage utilization"
        Matcher mi = CASE_STORAGE_UNITS.matcher(p.base);
        if (mi.matches()) {
            p.disk = mi.group(1).trim();
            p.base = mi.group(2).trim();
        }

        if (p.base.isEmpty()) p.base = "zabbix.metric";
        return p;
    }

    private static class NameParts {
        String base;
        String port;
        String host;
        String disk;
        String cpu;
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
