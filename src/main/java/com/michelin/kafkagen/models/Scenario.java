package com.michelin.kafkagen.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@RegisterForReflection
public class Scenario {
    private String apiVersion;
    private String kind;
    private ObjectMeta metadata;
    @JsonInclude(value = JsonInclude.Include.NON_ABSENT)
    private ScenarioSpec spec;

    @Getter
    @Setter
    @NoArgsConstructor
    @RegisterForReflection
    public static class ScenarioSpec {
        private String topic;
        private String datasetFile;
        private String templateFile;
        private String avroFile;
        private Integer maxInterval;
        private Long iterations;
        private Map<String, Map<String, Object>> variables;
    }
}
