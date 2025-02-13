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

package com.michelin.kafkagen.utils;

import com.fasterxml.jackson.core.base.GeneratorBase;
import com.google.protobuf.Descriptors;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.List;
import java.util.stream.Collectors;

public class ProtobufSampleUtils {

    private static final List<Descriptors.FieldDescriptor.Type> PRIMITIVE_TYPES =
        List.of(Descriptors.FieldDescriptor.Type.STRING, Descriptors.FieldDescriptor.Type.DOUBLE,
            Descriptors.FieldDescriptor.Type.INT32, Descriptors.FieldDescriptor.Type.INT64,
            Descriptors.FieldDescriptor.Type.FLOAT, Descriptors.FieldDescriptor.Type.ENUM,
            Descriptors.FieldDescriptor.Type.BOOL);

    /**
     * Create a sample record for Protobuf schema in JSON
     *
     * @param generator - the JSON generator
     * @param schema    - the Protobuf schema to use
     * @throws Exception
     */
    public static void generateSample(GeneratorBase generator, ProtobufSchema schema) throws Exception {
        generateMessageSample(generator, schema.toDescriptor().getFields());
    }

    /**
     * Generate an JsonSchema ObjectSchema sample.
     *
     * @param generator - the JSON generator
     * @param fields    - fields corresponding to the message
     * @throws Exception
     */
    public static void generateMessageSample(GeneratorBase generator, List<Descriptors.FieldDescriptor> fields)
        throws Exception {
        generator.writeStartObject();

        for (Descriptors.FieldDescriptor f : fields) {
            generator.writeFieldName(f.getName());
            generateFieldSample(generator, f);
        }

        generator.writeEndObject();
    }

    /**
     * Generate a field sample.
     *
     * @param generator - the JSON generator
     * @param field     - the field
     * @throws Exception
     */
    private static void generateFieldSample(GeneratorBase generator, Descriptors.FieldDescriptor field)
        throws Exception {
        if (field.isRepeated()) {
            generator.writeStartArray();
            if (PRIMITIVE_TYPES.contains(field.getType())) {
                generator.writeObject(generateSample(field));
                generator.writeObject(generateSample(field));
            } else if (field.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE)) {
                generateMessageSample(generator, field.getMessageType().getFields());
            }
            generator.writeEndArray();
        } else if (PRIMITIVE_TYPES.contains(field.getType())) {
            generator.writeObject(generateSample(field));
        } else if (field.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE)) {
            generateMessageSample(generator, field.getMessageType().getFields());
        }
    }

    /**
     * Generate the sample value depending on the type.
     *
     * @param field - the field
     * @return the sample value
     */
    static Object generateSample(Descriptors.FieldDescriptor field) {
        Object result = null;

        switch (field.getType()) {
            case STRING -> result = "String_value";
            case INT32, INT64, UINT32, UINT64, SINT32, SINT64, FIXED32, FIXED64, SFIXED32, SFIXED64 -> result = 42;
            case DOUBLE, FLOAT -> result = 42.1;
            case BOOL -> result = true;
            case ENUM -> result = field.getEnumType().getValues()
                .stream()
                .map(Descriptors.EnumValueDescriptor::getName)
                .collect(Collectors.joining("|"));
            default -> {
                // Do nothing
            }
        }

        return result;
    }
}
