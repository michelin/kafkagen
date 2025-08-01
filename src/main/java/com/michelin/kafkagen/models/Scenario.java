/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
        private Integer keySubjectVersion;
        private Integer valueSubjectVersion;
        private String datasetFile;
        private String templateFile;
        private String avroFile;
        private Integer maxInterval;
        private Long iterations;
        private Map<String, Map<String, Object>> variables;
    }
}
