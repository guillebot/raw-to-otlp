package com.gstechs.kafkastreams.mappers;

public interface OtlpProtoMapper {
    byte[] toOtlpProto(String inputJson, String inputTopic) throws Exception;
}