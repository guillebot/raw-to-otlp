package com.gstechs.kafkastreams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import io.opentelemetry.proto.metrics.v1.*;
import io.opentelemetry.proto.common.v1.*;
import io.opentelemetry.proto.resource.v1.*;
import io.opentelemetry.proto.collector.metrics.v1.*;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.TopologyConfig; // change for performance
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class SevOneToOTLPTransformer {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String configFilePath = "config.yaml"; // default config file path

        for (int i = 0; i < args.length - 1; i++) {
            if ("-c".equals(args[i])) {
                configFilePath = args[i + 1];
                System.out.println("-c option detected, loading file: " + configFilePath);
            } else
            {
                System.out.println("Loading configurations from default config.yaml file.");
            }
        }

        Properties fileProps = new Properties();
        try (FileInputStream fis = new FileInputStream(configFilePath)) {
            fileProps.load(fis);
        }

        String bootstrapServers = fileProps.getProperty("bootstrap.servers");
        String applicationId    = fileProps.getProperty("application.id");
        String inputTopic = fileProps.getProperty("input.topic");
        String outputTopic = fileProps.getProperty("output.topic");
        double sampleRate = Double.parseDouble(fileProps.getProperty("sample.rate", "1.0"));
        String format = fileProps.getProperty("format", "json").toLowerCase();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // tuning aiming for more performance
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put("cache.max.bytes.buffering", 10485760); // 10MB
        props.put("buffered.records.per.partition", 1000);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        //

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(inputTopic);

        if (format.equals("protobuf")) {
            input.filter((key, value) -> random.nextDouble() < sampleRate)
                 .mapValues(value -> convertToOtlpProtobuf(value, inputTopic))
                 .to(outputTopic, Produced.with(Serdes.String(), Serdes.ByteArray()));
        } else {
            input.filter((key, value) -> random.nextDouble() < sampleRate)
                 .mapValues(value -> convertToOtlpJson(value, inputTopic))
                 .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String convertToOtlpJson(String sevOneJson, String inputTopic) {
        try {
            JsonNode sevOne = mapper.readTree(sevOneJson);
            long timeNano = sevOne.get("time").asLong() * 1_000_000_000L;
            double value = Double.parseDouble(sevOne.get("value").asText());

            ObjectNode root = mapper.createObjectNode();
            ObjectNode resourceMetric = mapper.createObjectNode();
            ObjectNode resource = mapper.createObjectNode();
            ArrayNode resourceAttributes = mapper.createArrayNode();
            resourceAttributes.add(attr("cluster.name", sevOne.get("clusterName").asText()));
            resourceAttributes.add(attr("plugin.name", sevOne.get("pluginName").asText()));
            resourceAttributes.add(attr("kafka.topic", inputTopic));
            resource.set("attributes", resourceAttributes);

            resourceMetric.set("resource", resource);

            ObjectNode scopeMetric = mapper.createObjectNode();
            scopeMetric.putObject("scope").put("name", "kafka").put("version", "streams");
            ArrayNode metrics = mapper.createArrayNode();
            ObjectNode metric = mapper.createObjectNode();
            metric.put("name", sevOne.get("indicatorName").asText());
            metric.put("unit", sevOne.get("units").asText());
            metric.put("type", "gauge");

            ObjectNode gauge = mapper.createObjectNode();
            ArrayNode dataPoints = mapper.createArrayNode();
            ObjectNode point = mapper.createObjectNode();
            point.put("asDouble", value);
            point.put("timeUnixNano", String.valueOf(timeNano));

            ArrayNode pointAttributes = mapper.createArrayNode();
            pointAttributes.add(attr("device.name", sevOne.get("deviceName").asText()));
            pointAttributes.add(attr("device.ip", sevOne.get("deviceIp").asText()));
            pointAttributes.add(attr("object.name", sevOne.get("objectName").asText()));
            pointAttributes.add(attr("object.description", sevOne.get("objectDesc").asText()));
            pointAttributes.add(attr("cluster.name", sevOne.get("clusterName").asText()));
            pointAttributes.add(attr("plugin.name", sevOne.get("pluginName").asText()));
            pointAttributes.add(attr("kafka.topic", inputTopic));
            point.set("attributes", pointAttributes);

            dataPoints.add(point);
            gauge.set("dataPoints", dataPoints);

            metric.set("gauge", gauge);
            metrics.add(metric);

            scopeMetric.set("metrics", metrics);
            ArrayNode scopeMetrics = mapper.createArrayNode();
            scopeMetrics.add(scopeMetric);
            resourceMetric.set("scopeMetrics", scopeMetrics);

            ArrayNode resourceMetrics = mapper.createArrayNode();
            resourceMetrics.add(resourceMetric);
            root.set("resourceMetrics", resourceMetrics);

            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            e.printStackTrace();
            return "{}";
        }
    }

    private static byte[] convertToOtlpProtobuf(String sevOneJson, String inputTopic) {
        try {
            JsonNode sevOne = mapper.readTree(sevOneJson);
            long timeNano = sevOne.get("time").asLong() * 1_000_000_000L;
            double value = Double.parseDouble(sevOne.get("value").asText());

            KeyValue attr1 = KeyValue.newBuilder().setKey("device.name")
                .setValue(AnyValue.newBuilder().setStringValue(sevOne.get("deviceName").asText()).build()).build();
            KeyValue attr2 = KeyValue.newBuilder().setKey("device.ip")
                .setValue(AnyValue.newBuilder().setStringValue(sevOne.get("deviceIp").asText()).build()).build();
            KeyValue attr3 = KeyValue.newBuilder().setKey("object.name")
                .setValue(AnyValue.newBuilder().setStringValue(sevOne.get("objectName").asText()).build()).build();
            KeyValue attr4 = KeyValue.newBuilder().setKey("object.description")
                .setValue(AnyValue.newBuilder().setStringValue(sevOne.get("objectDesc").asText()).build()).build();

            NumberDataPoint point = NumberDataPoint.newBuilder()
                .addAttributes(attr1)
                .addAttributes(attr2)
                .addAttributes(attr3)
                .addAttributes(attr4)
                .setTimeUnixNano(timeNano)
                .setAsDouble(value)
                .build();

            Gauge gauge = Gauge.newBuilder()
                .addDataPoints(point)
                .build();

            Metric metric = Metric.newBuilder()
                .setName(sevOne.get("indicatorName").asText())
                .setUnit(sevOne.get("units").asText())
                .setGauge(gauge)
                .build();

            InstrumentationScope scope = InstrumentationScope.newBuilder()
                .setName("kafka")
                .setVersion("streams")
                .build();

            ScopeMetrics scopeMetrics = ScopeMetrics.newBuilder()
                .setScope(scope)
                .addMetrics(metric)
                .build();

            KeyValue resAttr1 = KeyValue.newBuilder().setKey("cluster.name")
                .setValue(AnyValue.newBuilder().setStringValue(sevOne.get("clusterName").asText()).build()).build();
            KeyValue resAttr2 = KeyValue.newBuilder().setKey("plugin.name")
                .setValue(AnyValue.newBuilder().setStringValue(sevOne.get("pluginName").asText()).build()).build();
            KeyValue resAttr3 = KeyValue.newBuilder().setKey("kafka.topic")
                .setValue(AnyValue.newBuilder().setStringValue(inputTopic).build()).build();

            Resource resource = Resource.newBuilder()
                .addAttributes(resAttr1)
                .addAttributes(resAttr2)
                .addAttributes(resAttr3)
                .build();

            ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addScopeMetrics(scopeMetrics)
                .build();

            ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics)
                .build();

            return request.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    private static ObjectNode attr(String key, String value) {
        ObjectNode attr = mapper.createObjectNode();
        attr.put("key", key);
        ObjectNode val = mapper.createObjectNode();
        val.put("stringValue", value);
        attr.set("value", val);
        return attr;
    }
}
