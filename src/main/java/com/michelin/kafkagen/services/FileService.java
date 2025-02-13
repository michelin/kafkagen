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

import com.michelin.kafkagen.exceptions.FileFormatException;
import com.michelin.kafkagen.models.Scenario;
import jakarta.inject.Singleton;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Singleton
public class FileService {

    public List<File> computeYamlFileList(File fileOrDirectory, boolean recursive) {
        return listAllFiles(new File[] {fileOrDirectory}, recursive)
            .collect(Collectors.toList());
    }

    public List<Scenario> parseResourceListFromFiles(List<File> files) {
        return files.stream()
            .map(File::toPath)
            .map(path -> {
                try {
                    return Files.readString(path);
                } catch (Exception e) {
                    // checked to unchecked
                    throw new FileFormatException(path.toString());
                }
            })
            .flatMap(this::parseResourceStreamFromString)
            .collect(Collectors.toList());
    }

    public List<Scenario> parseResourceListFromString(String content) {
        return parseResourceStreamFromString(content).collect(Collectors.toList());
    }

    private Stream<Scenario> parseResourceStreamFromString(String content) {
        var loaderOptions = new LoaderOptions();
        var yaml = new Yaml(new Constructor(Scenario.class, loaderOptions));
        return StreamSupport.stream(yaml.loadAll(content).spliterator(), false)
            .map(o -> (Scenario) o);
    }

    private Stream<File> listAllFiles(File[] rootDir, boolean recursive) {
        return Arrays.stream(rootDir)
            .flatMap(currentElement -> {
                if (currentElement.isDirectory()) {
                    File[] files = currentElement.listFiles(
                        file -> file.isFile() && (file.getName().endsWith(".yaml") || file.getName().endsWith(".yml")));
                    Stream<File> directories =
                        recursive ? listAllFiles(currentElement.listFiles(File::isDirectory), true) : Stream.empty();
                    return Stream.concat(Stream.of(files), directories);
                } else {
                    return Stream.of(currentElement);
                }
            });
    }
}
