package com.gstechs.kafkastreams.mappers;

public interface OtlpJsonMapper {
    String toOtlpJson(String inputJson, String inputTopic) throws Exception;
}