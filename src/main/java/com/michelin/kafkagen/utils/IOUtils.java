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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.core.json.WriterBasedJsonGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;

public class IOUtils {

    public static GeneratorBase getGenerator(String format, StringWriter out) {
        GeneratorBase generator;

        try {
            switch (format) {
                case "yml", "yaml" -> {
                    YAMLFactory factory = new YAMLFactory();
                    factory.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
                    generator = factory.createGenerator(out);
                }
                case "json" -> {
                    JsonFactory factory = new JsonFactory();
                    generator = (WriterBasedJsonGenerator) factory.createGenerator(out);
                }
                default -> throw new IllegalArgumentException("Invalid format: " + format);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        generator.useDefaultPrettyPrinter();

        return generator;
    }

    public static void writeOutToFile(File file, StringWriter out) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(out.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
