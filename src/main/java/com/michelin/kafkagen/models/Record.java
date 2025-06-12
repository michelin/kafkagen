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
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer keySubjectVersion;
    private Object key;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer valueSubjectVersion;
    private Object value;
}
