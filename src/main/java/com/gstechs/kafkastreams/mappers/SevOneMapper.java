package com.gstechs.kafkastreams.mappers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;



/**
 * Maps SevOne JSON metrics into OTLP JSON or OTLP Protobuf.
 * Implements both OtlpJsonMapper and OtlpProtoMapper so callers can choose encoding at runtime.
 */
public class SevOneMapper implements OtlpJsonMapper, OtlpProtoMapper {
    private static final ObjectMapper M = new ObjectMapper();

    @Override
    public String toOtlpJson(String sevOneJson, String inputTopic) throws Exception {
        JsonNode s = M.readTree(sevOneJson);
        long timeUnixNano = s.path("time").asLong() * 1_000_000_000L;
        double value = Double.parseDouble(s.path("value").asText());

        ObjectNode root = M.createObjectNode();
        ObjectNode resourceMetric = M.createObjectNode();
        ObjectNode resource = M.createObjectNode();
        ArrayNode resourceAttributes = M.createArrayNode();
        resourceAttributes.add(attr("cluster.name", s.path("clusterName").asText("")));
        resourceAttributes.add(attr("plugin.name",  s.path("pluginName").asText("")));
        resourceAttributes.add(attr("kafka.topic",  inputTopic));
        resource.set("attributes", resourceAttributes);
        resourceMetric.set("resource", resource);

        ObjectNode scopeMetric = M.createObjectNode();
        scopeMetric.putObject("scope").put("name", "kafka").put("version", "streams");
        ArrayNode metrics = M.createArrayNode();
        ObjectNode metric = M.createObjectNode();
        metric.put("name", s.path("indicatorName").asText("sevone.metric"));
        metric.put("unit", s.path("units").asText(""));
        ObjectNode gauge = M.createObjectNode();
        ArrayNode dataPoints = M.createArrayNode();
        ObjectNode point = M.createObjectNode();
        point.put("asDouble", value);
        point.put("timeUnixNano", timeUnixNano);

        ArrayNode pointAttributes = M.createArrayNode();
        pointAttributes.add(attr("device.name", s.path("deviceName").asText("")));
        pointAttributes.add(attr("device.ip",   s.path("deviceIp").asText("")));
        pointAttributes.add(attr("object.name", s.path("objectName").asText("")));
        pointAttributes.add(attr("object.description", s.path("objectDesc").asText("")));
        pointAttributes.add(attr("cluster.name", s.path("clusterName").asText("")));
        pointAttributes.add(attr("plugin.name",  s.path("pluginName").asText("")));
        pointAttributes.add(attr("kafka.topic",  inputTopic));
        point.set("attributes", pointAttributes);

        dataPoints.add(point);
        gauge.set("dataPoints", dataPoints);
        metric.set("gauge", gauge);
        metrics.add(metric);

        scopeMetric.set("metrics", metrics);
        ArrayNode scopeMetrics = M.createArrayNode();
        scopeMetrics.add(scopeMetric);
        resourceMetric.set("scopeMetrics", scopeMetrics);

        ArrayNode resourceMetrics = M.createArrayNode();
        resourceMetrics.add(resourceMetric);
        root.set("resourceMetrics", resourceMetrics);
        return M.writeValueAsString(root);
    }

    @Override
    public byte[] toOtlpProto(String sevOneJson, String inputTopic) throws Exception {
        JsonNode s = M.readTree(sevOneJson);
        long timeUnixNano = s.path("time").asLong() * 1_000_000_000L;
        double value = Double.parseDouble(s.path("value").asText());

        KeyValue attr1 = KeyValue.newBuilder().setKey("device.name")
                .setValue(AnyValue.newBuilder().setStringValue(s.path("deviceName").asText(""))).build();
        KeyValue attr2 = KeyValue.newBuilder().setKey("device.ip")
                .setValue(AnyValue.newBuilder().setStringValue(s.path("deviceIp").asText(""))).build();
        KeyValue attr3 = KeyValue.newBuilder().setKey("object.name")
                .setValue(AnyValue.newBuilder().setStringValue(s.path("objectName").asText(""))).build();
        KeyValue attr4 = KeyValue.newBuilder().setKey("object.description")
                .setValue(AnyValue.newBuilder().setStringValue(s.path("objectDesc").asText(""))).build();

        NumberDataPoint point = NumberDataPoint.newBuilder()
                .addAttributes(attr1)
                .addAttributes(attr2)
                .addAttributes(attr3)
                .addAttributes(attr4)
                .setTimeUnixNano(timeUnixNano)
                .setAsDouble(value)
                .build();

        Gauge gauge = Gauge.newBuilder().addDataPoints(point).build();
        Metric metric = Metric.newBuilder()
                .setName(s.path("indicatorName").asText("sevone.metric"))
                .setUnit(s.path("units").asText(""))
                .setGauge(gauge)
                .build();

        InstrumentationScope scope = InstrumentationScope.newBuilder()
                .setName("kafka").setVersion("streams").build();
        io.opentelemetry.proto.metrics.v1.ScopeMetrics scopeMetrics =
                io.opentelemetry.proto.metrics.v1.ScopeMetrics.newBuilder()
                        .setScope(scope)
                        .addMetrics(metric)
                        .build();

        KeyValue resAttr1 = KeyValue.newBuilder().setKey("cluster.name")
                .setValue(AnyValue.newBuilder().setStringValue(s.path("clusterName").asText(""))).build();
        KeyValue resAttr2 = KeyValue.newBuilder().setKey("plugin.name")
                .setValue(AnyValue.newBuilder().setStringValue(s.path("pluginName").asText(""))).build();
        KeyValue resAttr3 = KeyValue.newBuilder().setKey("kafka.topic")
                .setValue(AnyValue.newBuilder().setStringValue(inputTopic)).build();

        Resource resource = Resource.newBuilder()
                .addAttributes(resAttr1)
                .addAttributes(resAttr2)
                .addAttributes(resAttr3)
                .build();

        io.opentelemetry.proto.metrics.v1.ResourceMetrics resourceMetrics =
                io.opentelemetry.proto.metrics.v1.ResourceMetrics.newBuilder()
                        .setResource(resource)
                        .addScopeMetrics(scopeMetrics)
                        .build();

        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics)
                .build()
                .toByteArray();
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
