package com.michelin.kafkagen.models;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.kafka.common.serialization.Serializer;

@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Dataset {
    private String topic;
    @Builder.Default
    private List<Record> records = new ArrayList<>();
    @SuppressWarnings("rawtypes")
    private Class<? extends Serializer> keySerializer;
    @SuppressWarnings("rawtypes")
    private Class<? extends Serializer> valueSerializer;
}
