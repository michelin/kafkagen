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

import com.michelin.kafkagen.models.Record;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
public class DatasetServiceTest {

    @Inject
    DatasetService datasetService;

    @Test
    public void getRawRecordsJson() throws Exception {
        List<Record> records = datasetService.getRawRecord(new File(getClass().getClassLoader().getResource("avro/datasets/dataset.json").toURI()));
        assertEquals(2, records.size());
        assertEquals(Map.of("keyFieldString", "key1", "keyFieldInt",42), records.getFirst().getKey());
        assertEquals(Map.of("fieldString", "value1",
                        "fieldInt", 42,
                        "fieldBool", true,
                        "fieldLong", 42,
                        "fieldFloat", 42.1,
                        "fieldDouble", 42.1,
                        "fieldUnion", Map.of("string", "String_value"),
                        "fieldArray", List.of("String_value"),
                        "fieldRecord", Map.of("fieldRecordString", "String_value",
                                "fieldRecordInt", 42)),
                records.getFirst().getValue());

        assertEquals(Map.of("keyFieldString", "key2", "keyFieldInt",42), records.get(1).getKey());
        assertEquals(Map.of("fieldString", "value2",
                        "fieldInt", 42,
                        "fieldBool", true,
                        "fieldLong", 42,
                        "fieldFloat", 42.1,
                        "fieldDouble", 42.1,
                        "fieldUnion", Map.of("string", "String_value"),
                        "fieldArray", List.of("String_value"),
                        "fieldRecord", Map.of("fieldRecordString", "String_value",
                                "fieldRecordInt", 42)),
                records.get(1).getValue());
    }
    @Test
    public void getRawRecordsYAML() throws Exception {
        List<Record> records = datasetService.getRawRecord(new File(getClass().getClassLoader().getResource("avro/datasets/dataset.yaml").toURI()));
        assertEquals(2, records.size());
        assertEquals(Map.of("keyFieldString", "key1", "keyFieldInt",42), records.getFirst().getKey());
        assertEquals(Map.of("fieldString", "value1",
                        "fieldInt", 42,
                        "fieldBool", true,
                        "fieldLong", 42,
                        "fieldFloat", 42.1,
                        "fieldDouble", 42.1,
                        "fieldUnion", Map.of("string", "String_value"),
                        "fieldArray", List.of("String_value"),
                        "fieldRecord", Map.of("fieldRecordString", "String_value",
                                "fieldRecordInt", 42)),
                records.getFirst().getValue());

        assertEquals(Map.of("keyFieldString", "key2", "keyFieldInt",42), records.get(1).getKey());
        assertEquals(Map.of("fieldString", "value2",
                        "fieldInt", 42,
                        "fieldBool", true,
                        "fieldLong", 42,
                        "fieldFloat", 42.1,
                        "fieldDouble", 42.1,
                        "fieldUnion", Map.of("string", "String_value"),
                        "fieldArray", List.of("String_value"),
                        "fieldRecord", Map.of("fieldRecordString", "String_value",
                                "fieldRecordInt", 42)),
                records.get(1).getValue());
    }

    @Test
    public void compactWithStringKey() {
        List<Record> records = List.of(
                Record.builder().key("key1")
                        .value(Map.of("fieldString", "value1")).build(),
                Record.builder().key("key2")
                        .value(Map.of("fieldString", "value2")).build(),
                Record.builder().key("key1")
                        .value(null).build(),
                Record.builder().key("key3")
                        .value(Map.of("fieldString", "value3")).build(),
                Record.builder().key("key3")
                        .value(null).build(),
                Record.builder().key("key4")
                        .value(Map.of("fieldString", "value4")).build(),
                Record.builder().key("key3")
                        .value(Map.of("fieldString", "value3_2")).build()
        );
        List<Record> compactedRecords = datasetService.compact(records);
        assertEquals(3, compactedRecords.size());
        assertEquals(List.of("key2", "key3", "key4"), compactedRecords.stream().map(Record::getKey).toList());
        assertEquals(Map.of("fieldString", "value3_2"),
                compactedRecords.stream().filter(r -> r.getKey() == "key3").toList().getFirst().getValue());
    }

    @Test
    public void compactWithAvroKey() {
        List<Record> records = List.of(
                Record.builder().key(Map.of("keyFieldString", "key1", "keyFieldInt", 42))
                        .value(Map.of("fieldString", "value1")).build(),
                Record.builder().key(Map.of("keyFieldString", "key2", "keyFieldInt", 42))
                        .value(Map.of("fieldString", "value2")).build(),
                Record.builder().key(Map.of("keyFieldString", "key1", "keyFieldInt", 42))
                        .value(null).build(),
                Record.builder().key(Map.of("keyFieldString", "key3", "keyFieldInt", 42))
                        .value(Map.of("fieldString", "value3")).build(),
                Record.builder().key(Map.of("keyFieldString", "key3", "keyFieldInt", 42))
                        .value(null).build(),
                Record.builder().key(Map.of("keyFieldString", "key4", "keyFieldInt", 42))
                        .value(Map.of("fieldString", "value4")).build(),
                Record.builder().key(Map.of("keyFieldString", "key3", "keyFieldInt", 42))
                        .value(Map.of("fieldString", "value3_2")).build()
        );
        List<Record> compactedRecords = datasetService.compact(records);
        assertEquals(3, compactedRecords.size());
        assertEquals(List.of(Map.of("keyFieldString", "key4", "keyFieldInt", 42),
                        Map.of("keyFieldString", "key3", "keyFieldInt", 42),
                        Map.of("keyFieldString", "key2", "keyFieldInt", 42)),
                compactedRecords.stream().map(Record::getKey).toList());
        assertEquals(Map.of("fieldString", "value3_2"),
                compactedRecords.stream().filter(r ->
                        Objects.equals(r.getKey(), Map.of("keyFieldString", "key3", "keyFieldInt", 42)))
                        .toList().getFirst().getValue());
    }

    @Test
    public void getRawRecordsForFileWithWrongExtension() {
        assertThrows(RuntimeException.class, () -> datasetService.getRawRecord(new File("dataset")));
        assertThrows(RuntimeException.class, () -> datasetService.getRawRecord(new File("dataset.xml")));
    }
}
