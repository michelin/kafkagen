package com.michelin.kafkagen.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
@RegisterForReflection
public class Record {
    @Builder.Default
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, String> headers = Map.of();
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String topic;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long timestamp;
    @JsonIgnore
    private long offset;
    // Indicates if the expected record is the most recent (to test multiple versions on compacted topics)
    @Builder.Default
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Boolean mostRecent = false;
    private Object key;
    private Object value;
}
