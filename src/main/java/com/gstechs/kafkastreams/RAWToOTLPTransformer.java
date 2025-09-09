package com.gstechs.kafkastreams;

import com.gstechs.kafkastreams.mappers.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class RAWToOTLPTransformer {

    private static final Random random = new Random();

    public static void main(String[] args) throws IOException {
        String configFilePath = "config.properties";
        for (int i = 0; i < args.length - 1; i++) {
            if ("-c".equals(args[i])) {
                configFilePath = args[i + 1];
                System.out.println("-c option detected, loading file: " + configFilePath);
            } else {
                System.out.println("Loading configurations from default config.properties file.");
            }
        }

        Properties fileProps = new Properties();
        try (FileInputStream fis = new FileInputStream(configFilePath)) {
            fileProps.load(fis);
        }

        String bootstrapServers = fileProps.getProperty("bootstrap.servers");
        String applicationId    = fileProps.getProperty("application.id", "otlp-transformer-app");
        String inputTopic       = fileProps.getProperty("input.topic");
        String outputTopic      = fileProps.getProperty("output.topic");
        double sampleRate       = Double.parseDouble(fileProps.getProperty("sample.rate", "1.0"));
        String format           = fileProps.getProperty("format", "json").toLowerCase();
        String source           = fileProps.getProperty("source"); // no default, must be provided
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration: source");
        }
        source = source.toLowerCase();        

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put("cache.max.bytes.buffering", 10 * 1024 * 1024);
        props.put("buffered.records.per.partition", 1000);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        OtlpJsonMapper jsonMapper;
        OtlpProtoMapper protoMapper = null;
        switch (source) {
            case "netscout" -> jsonMapper = new NetscoutMapper();
            case "zabbix" -> {
                jsonMapper = new ZabbixMapper();
            }
            case "sevone" -> {
                jsonMapper = new SevOneMapper();
                protoMapper = (OtlpProtoMapper) jsonMapper;
            }
            default -> throw new IllegalArgumentException("Unsupported source: " + source);
        }

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(inputTopic);

        if ("protobuf".equals(format)) {
            final OtlpProtoMapper pm = protoMapper;
            input.filter((key, value) -> random.nextDouble() < sampleRate)
                 .mapValues(value -> {
                     if (pm == null) return new byte[0];
                     try { return pm.toOtlpProto(value, inputTopic); } catch (Exception e) { e.printStackTrace(); return new byte[0]; }
                 })
                 .to(outputTopic, Produced.with(Serdes.String(), Serdes.ByteArray()));
        } else {
            final OtlpJsonMapper jm = jsonMapper;
            input.filter((key, value) -> random.nextDouble() < sampleRate)
                 .mapValues(value -> {
                     try { return jm.toOtlpJson(value, inputTopic); } catch (Exception e) { e.printStackTrace(); return "{}"; }
                 })
                 .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}