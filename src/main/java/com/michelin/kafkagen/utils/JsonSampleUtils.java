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
import io.confluent.kafka.schemaregistry.ParsedSchema;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

public class JsonSampleUtils {

    private static final List<Class<?>> PRIMITIVE_CLASSES = List.of(NumberSchema.class, StringSchema.class,
        BooleanSchema.class, EnumSchema.class);

    /**
     * Create a sample record for JsonSchema schema in JSON.
     *
     * @param generator - the JSON generator
     * @param schema    - the JSON schema to use
     * @throws Exception
     */
    public static void generateSample(GeneratorBase generator, ParsedSchema schema) throws Exception {
        generateObjectSchemaSample(generator, findFirstObjectSchema((Schema) schema.rawSchema()));
    }

    /**
     * Generate an JsonSchema ObjectSchema sample.
     *
     * @param generator - the JSON generator
     * @param schema    - the schema to generate the sample from
     * @throws Exception
     */
    public static void generateObjectSchemaSample(GeneratorBase generator, ObjectSchema schema) throws Exception {
        generator.writeStartObject();
        Map<String, Schema> fields = schema.getPropertySchemas();

        for (Map.Entry<String, org.everit.json.schema.Schema> f : fields.entrySet()) {
            generator.writeFieldName(f.getKey());
            generateFieldSample(generator, f.getValue());
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
    private static void generateFieldSample(GeneratorBase generator, Schema field) throws Exception {
        if (PRIMITIVE_CLASSES.contains(field.getClass())) {
            generator.writeObject(generateSample(field));
        } else if (field instanceof ArraySchema arrayField) {
            generator.writeStartArray();
            if (PRIMITIVE_CLASSES.contains(arrayField.getAllItemSchema().getClass())) {
                generator.writeObject(generateSample(arrayField.getAllItemSchema()));
                generator.writeObject(generateSample(arrayField.getAllItemSchema()));
            } else if (arrayField.getAllItemSchema().getClass().equals(ObjectSchema.class)) {
                generateObjectSchemaSample(generator, (ObjectSchema) arrayField.getAllItemSchema());
            }
            generator.writeEndArray();
        } else if (field instanceof ObjectSchema objectField) {
            generateObjectSchemaSample(generator, objectField);
        } else if (field instanceof ReferenceSchema referenceField) {
            generateFieldSample(generator, referenceField.getReferredSchema());
        }
    }

    /**
     * Generate the sample value depending on the type.
     *
     * @param fieldClass - the field type
     * @return the sample value
     */
    static Object generateSample(Schema fieldClass) {
        Object result = null;

        switch (fieldClass.getClass().getSimpleName()) {
            case "StringSchema" -> result = "String_value";
            case "NumberSchema" -> {
                if (((NumberSchema) fieldClass).requiresInteger()) {
                    result = Integer.valueOf(42);
                } else {
                    result = 42.1;
                }
            }
            case "BooleanSchema" -> result = true;
            case "EnumSchema" ->
                    result = ((EnumSchema) fieldClass).getPossibleValues()
                        .stream().map(Object::toString).collect(Collectors.joining("|"));
        }

        return result;
    }

    /**
     * Navigate into the combined or reference schemas to get a schema that we can generate a sample from.
     *
     * @param schema - the input schema
     * @return an ObjectSchema to generate the sample from
     */
    public static ObjectSchema findFirstObjectSchema(Schema schema) {
        if (schema instanceof ObjectSchema) {
            return (ObjectSchema) schema;
        }

        if (schema instanceof CombinedSchema) {
            Optional<Schema> optionalSchema = ((CombinedSchema) schema).getSubschemas().stream().findFirst();
            return optionalSchema.map(JsonSampleUtils::findFirstObjectSchema).orElse(null);
        }

        if (schema instanceof ReferenceSchema) {
            return findFirstObjectSchema(((ReferenceSchema) schema).getReferredSchema());
        }

        return null;
    }
}
