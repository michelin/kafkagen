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
import com.fasterxml.jackson.core.json.WriterBasedJsonGenerator;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;

public class AvroUtils {

    /**
     * Create a sample record for an Avro schema in JSON.
     *
     * @param generator - the JSON generator
     * @param schema    - the Avro schema to use
     * @param options   - options such as pretty print or fields documentation
     * @throws Exception
     */
    public static void generateSample(GeneratorBase generator, ParsedSchema schema, Map<String, Boolean> options)
        throws Exception {
        Schema avroSchema = ((AvroSchema) schema).rawSchema();

        switch (avroSchema.getType()) {
            case RECORD -> {
                var generatedRecordsName = new ArrayList<String>();
                generatedRecordsName.add(((Schema) schema.rawSchema()).getName());
                generateRecordSample(generator, generatedRecordsName, avroSchema.getFields(), options);
            }
            case STRING -> generator.writeString("String_value");
            // TODO: support the other types
        }
    }

    /**
     * Generate an Avro Schema.Type.Record sample.
     *
     * @param generator            - the JSON generator
     * @param generatedRecordsName - list of already generated records name to prevent infinite generation in case
     *                             of recursion
     * @param fields               - fields of the Avro record
     * @param options              - options such as pretty print or fields documentation
     * @throws Exception
     */
    public static void generateRecordSample(GeneratorBase generator, List<String> generatedRecordsName,
                                            List<Schema.Field> fields, Map<String, Boolean> options) throws Exception {
        generator.writeStartObject();

        for (org.apache.avro.Schema.Field f : fields) {
            generator.writeFieldName(f.name());
            generateFieldSample(generator, generatedRecordsName, f, f.pos() == fields.size() - 1, options);
        }

        generator.writeEndObject();
    }

    /**
     * Generate a field sample.
     *
     * @param generator            - the JSON generator
     * @param generatedRecordsName - list of already generated records name to prevent infinite generation in case
     *                             of recursion
     * @param field                - the field
     * @param lastField            - true if it's the last field (use for doc inclusion in the sample)
     * @param options              - options such as pretty print or fields documentation
     * @throws Exception
     */
    private static void generateFieldSample(GeneratorBase generator, List<String> generatedRecordsName,
                                            Schema.Field field, boolean lastField, Map<String, Boolean> options)
        throws Exception {
        Schema schema = field.schema();
        Schema.Type type = field.schema().getType();

        // For union, handle the type specification based on --pretty argument
        if (field.schema().getType().equals(Schema.Type.UNION)) {
            Optional<Schema> optionalSchema = field.schema().getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst();

            if (optionalSchema.isPresent()) {
                schema = optionalSchema.get();
            }

            if (options.get("pretty")) {
                type = schema.getType();
            } else if (schema.getType().equals(Schema.Type.RECORD)) {
                Schema.Field f = new Schema.Field(schema.getType().toString().toLowerCase(), schema);
                generateFieldSample(generator, generatedRecordsName, f, lastField, options);
                return;
            } else {
                generateRecordSample(generator, generatedRecordsName,
                    List.of(new Schema.Field(schema.getType().toString().toLowerCase(), schema)), options);
                return;
            }
        }

        switch (type) {
            case STRING, INT, BOOLEAN, DOUBLE, FLOAT, LONG, BYTES -> {
                generator.writeObject(generateSample(type, schema.getLogicalType(), options.get("pretty")));
                addDoc(generator, field.doc(), lastField, options.get("withDoc"));
            }
            case ENUM -> {
                generator.writeString(String.join("|", schema.getEnumSymbols()));
                addDoc(generator, field.doc(), lastField, options.get("withDoc"));
            }
            case ARRAY -> {
                generator.writeStartArray();
                switch (schema.getElementType().getType()) {
                    case RECORD -> {
                        // Prevent infinite generation in case of recursion if we already generated this type of record
                        if (!generatedRecordsName.contains(schema.getElementType().getName())) {
                            // Going deeper in the hierarchy, clone the list to keep record generation for records
                            // in this branch on the other branches
                            var subGeneratedRecordsName = new ArrayList<>(generatedRecordsName);
                            subGeneratedRecordsName.add(schema.getElementType().getName());
                            generateRecordSample(generator, subGeneratedRecordsName,
                                schema.getElementType().getFields(),
                                options);
                        }
                    }
                    case ENUM -> {
                        Schema.Field f =
                            new Schema.Field(schema.getType().toString().toLowerCase(), schema.getElementType());
                        generateFieldSample(generator, generatedRecordsName, f, false,
                            options);
                    }
                    default -> {
                        generator.writeObject(
                            generateSample(schema.getElementType().getType(), schema.getElementType().getLogicalType(),
                                options.get("pretty")));
                    }
                }
                generator.writeEndArray();
            }
            case RECORD -> {
                // Prevent infinite generation in case of recursion if we already generated this type of record
                if (!generatedRecordsName.contains(schema.getName())) {
                    // Going deeper in the hierarchy, clone the list to keep record generation for records in this
                    // branch on the other branches
                    var subGeneratedRecordsName = new ArrayList<>(generatedRecordsName);
                    subGeneratedRecordsName.add(schema.getName());
                    generateRecordSample(generator, subGeneratedRecordsName, schema.getFields(), options);
                } else {
                    generator.writeObject(generateSample(Schema.Type.NULL, null, options.get("pretty")));
                }
            }
            case MAP -> {
                generator.writeStartObject();
                generator.writeFieldName("key");
                generator.writeObject(generateSample(schema.getValueType().getType(), null, options.get("pretty")));
                generator.writeEndObject();
            }
            case FIXED -> {
                var byteArray = new byte[schema.getFixedSize()];
                Arrays.fill(byteArray, (byte) 97);
                generator.writeObject(byteArray);
            }
            default -> {
                throw new RuntimeException("Type not supported: " + type);
            }
        }
    }

    /**
     * Add the field doc in the sample.
     *
     * @param generator - the JSON generator
     * @param doc       - the doc to add in the sample
     * @param lastField - true if it's the last field
     * @param withDoc   - true if doc has to be added
     * @throws IOException
     */
    private static void addDoc(GeneratorBase generator, String doc, boolean lastField, boolean withDoc)
        throws IOException {
        if (withDoc && doc != null) {
            if (generator.getClass().equals(WriterBasedJsonGenerator.class)) {
                generator.writeRaw((lastField ? "  // " : ",  // ") + doc);
            }
        }
    }

    /**
     * Generate the sample value depending on the type.
     *
     * @param type        - the type
     * @param logicalType
     * @return the sample value
     */
    public static Object generateSample(Schema.Type type, LogicalType logicalType, boolean pretty) {
        Object result;

        switch (type) {
            case STRING -> {
                if (logicalType != null && logicalType.getName().equals("uuid")) {
                    result = UUID.randomUUID().toString();
                } else {
                    result = "String_value";
                }
            }
            case INT -> {
                if (logicalType != null) {
                    switch (logicalType.getName()) {
                        case "date" -> result =
                            pretty ? LocalDate.now().toString() : LocalDate.now().toEpochDay();
                        case "time-millis" -> result =
                            pretty ? LocalTime.now().withNano(100000000).toString() :
                                LocalTime.now().toSecondOfDay() * 1000;
                        default -> result = 42;
                    }
                } else {
                    result = 42;
                }
            }
            case BOOLEAN -> result = true;
            case DOUBLE -> result = 42.1d;
            case FLOAT -> result = 42.1f;
            case LONG -> {
                if (logicalType != null) {
                    switch (logicalType.getName()) {
                        case "time-micros" -> result =
                            pretty ? LocalTime.now().toString() : LocalTime.now().toSecondOfDay() * 1000000L;
                        case "timestamp-millis" -> result =
                            pretty ? LocalDateTime.now().withNano(100000000).atOffset(ZoneOffset.UTC).toString() :
                                LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) * 1000;
                        case "local-timestamp-millis" -> result =
                            pretty ? LocalDateTime.now().withNano(100000000).toString() :
                                LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) * 1000;
                        case "timestamp-micros" -> result =
                            pretty ? LocalDateTime.now().atOffset(ZoneOffset.UTC).toString() :
                                LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) * 1000000;
                        case "local-timestamp-micros" -> result =
                            pretty ? LocalDateTime.now().toString() :
                                LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) * 1000000;
                        default -> result = 42;
                    }
                } else {
                    result = 42;
                }
            }
            case BYTES -> {
                if (logicalType instanceof LogicalTypes.Decimal) {
                    result = "5.0000";
                } else {
                    result = "Bytes_value".getBytes();
                }
            }
            case NULL -> result = null;

            default -> throw new RuntimeException("Type not supported: " + type);
        }

        return result;
    }


    /**
     * Extract date and time fields from the given schema.
     *
     * @param schema The schema
     * @return The date, time and timestamp fields
     */
    public static Optional<Map<String, Object>> extractDateAndTimeFields(Schema schema) {
        var fields = new HashMap<String, Object>();

        if (schema.getType() == Schema.Type.RECORD) {
            schema.getFields().forEach(field -> {
                switch (field.schema().getType()) {
                    // Primitive types int and long, take the field if it has a logical type
                    case Schema.Type.INT, Schema.Type.LONG -> {
                        if (field.schema().getLogicalType() != null) {
                            fields.put(field.name(), field);
                        }
                    }
                    // Record type, need to traverse the schema
                    case RECORD -> extractDateAndTimeFields(field.schema())
                        .ifPresent(f -> fields.put(field.name(), f));
                    case ARRAY -> {
                        if ((field.schema().getElementType().getType() == Schema.Type.INT
                            || field.schema().getElementType().getType() == Schema.Type.LONG)
                            && field.schema().getElementType().getLogicalType() != null) {
                            fields.put(field.name(), field);
                        } else if (field.schema().getElementType().getType() == Schema.Type.RECORD) {
                            extractDateAndTimeFields(field.schema().getElementType())
                                .ifPresent(f -> fields.put(field.name(), f));
                        }
                    }
                    case UNION -> field.schema().getTypes().stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .forEach(type -> {
                            if ((type.getType() == Schema.Type.INT || type.getType() == Schema.Type.LONG)
                                && type.getLogicalType() != null) {
                                fields.put(field.name(), new Schema.Field(field.name(), type));
                            } else if (type.getType() == Schema.Type.RECORD) {
                                extractDateAndTimeFields(type).ifPresent(m -> fields.put(field.name(), m));
                            }
                        });
                }
            });
        }

        if (fields.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(fields);
        }
    }


    /**
     * Pretty print date and time fields for the given record.
     *
     * @param record The AVRO record
     */
    public static void prettyPrintDateAndTimeFields(GenericData.Record record) {
        Optional<Map<String, Object>> fields = AvroUtils.extractDateAndTimeFields(record.getSchema());

        fields.ifPresent(f -> f.forEach((key, value) -> {
            if (value instanceof Schema.Field v) {
                record.put(key, switch (v.schema().getLogicalType().getName()) {
                    case "timestamp-millis" -> new TimeConversions.TimestampMillisConversion().fromLong(
                        (Long) record.get(key), v.schema(), v.schema().getLogicalType());
                    case "local-timestamp-millis" -> new TimeConversions.LocalTimestampMillisConversion().fromLong(
                        (Long) record.get(key), v.schema(), v.schema().getLogicalType());
                    case "timestamp-micros" -> new TimeConversions.TimestampMicrosConversion().fromLong(
                        (Long) record.get(key), v.schema(), v.schema().getLogicalType());
                    case "local-timestamp-micros" -> new TimeConversions.LocalTimestampMicrosConversion().fromLong(
                        (Long) record.get(key), v.schema(), v.schema().getLogicalType());
                    case "date" -> new TimeConversions.DateConversion().fromInt((Integer) record.get(key),
                        v.schema(), v.schema().getLogicalType());
                    case "time-millis" -> new TimeConversions.TimeMillisConversion().fromInt(
                        (Integer) record.get(key), v.schema(), v.schema().getLogicalType());
                    case "time-micros" -> new TimeConversions.TimeMicrosConversion().fromLong(
                        (Long) record.get(key), v.schema(), v.schema().getLogicalType());
                    default -> null;
                });
            } else if (record.get(key) instanceof GenericData.Array) {
                ((GenericData.Array) record.get(key)).forEach(item -> {
                    if (item instanceof GenericData.Record) {
                        prettyPrintDateAndTimeFields((GenericData.Record) item);
                    }
                });
            } else if (record.get(key) instanceof GenericData.Record) {
                prettyPrintDateAndTimeFields((GenericData.Record) record.get(key));
            }
        }));
    }
}
