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

package com.michelin.kafkagen.services;

import com.fasterxml.jackson.core.base.GeneratorBase;
import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.utils.AvroUtils;
import com.michelin.kafkagen.utils.JsonSampleUtils;
import com.michelin.kafkagen.utils.ProtobufSampleUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

@Slf4j
@Singleton
public class SampleService {
    SchemaService schemaService;

    @Inject
    public SampleService(SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    public void generateSample(String topic, KafkagenConfig.Context context, GeneratorBase generator, boolean pretty,
                               boolean withDoc) throws Exception {
        ParsedSchema keySchema = null;
        ParsedSchema valueSchema = null;

        // Get key/value schema from registry to Avro serialization
        try {
            keySchema = schemaService.getLatestSchema(topic + "-key", context);
        } catch (Exception e) {
            log.trace("No key schema found for subject <{}>, continuing with String serialization", topic + "-key");
        }
        try {
            valueSchema = schemaService.getLatestSchema(topic + "-value", context);
        } catch (Exception e) {
            log.trace("No schema found for the given topic");
        }

        generator.writeStartArray();
        generator.writeStartObject();
        generator.writeFieldName("headers");
        generator.writeStartObject();
        generator.writeStringField("header1", "valueHeader1");
        generator.writeStringField("header2", "valueHeader2");
        generator.writeEndObject();

        generator.writeFieldName("key");

        generateSampleFromSchema(generator, keySchema, pretty, withDoc);

        generator.writeFieldName("value");

        generateSampleFromSchema(generator, valueSchema, pretty, withDoc);

        generator.writeEndObject();
        generator.writeEndArray();
        generator.close();
    }

    private void generateSampleFromSchema(GeneratorBase generator, ParsedSchema schema, boolean pretty,
                                          boolean withDoc) throws Exception {
        if (schema == null) {
            generator.writeObject(AvroUtils.generateSample(Schema.Type.STRING, null, pretty));
        } else {
            if (schema.getClass().equals(AvroSchema.class)) {
                AvroUtils.generateSample(generator, schema, Map.of("pretty", pretty, "withDoc", withDoc));
            } else if (schema.getClass().equals(JsonSchema.class)) {
                JsonSampleUtils.generateSample(generator, schema);
            } else if (schema.getClass().equals(ProtobufSchema.class)) {
                ProtobufSampleUtils.generateSample(generator, (ProtobufSchema) schema);
            }
        }
    }
}
