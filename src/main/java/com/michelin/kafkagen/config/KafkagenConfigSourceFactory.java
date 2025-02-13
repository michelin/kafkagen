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

package com.michelin.kafkagen.config;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.source.yaml.YamlConfigSource;
import java.net.URL;
import java.util.Collections;
import java.util.OptionalInt;
import org.eclipse.microprofile.config.spi.ConfigSource;

public class KafkagenConfigSourceFactory implements ConfigSourceFactory {

    @Override
    public Iterable<ConfigSource> getConfigSources(final ConfigSourceContext context) {
        YamlConfigSource yamlConfigSource = null;
        try {
            if (System.getenv().keySet().stream().noneMatch(s -> s.startsWith("KAFKAGEN_"))) {
                yamlConfigSource = new YamlConfigSource(
                    new URL("file:///" + System.getProperty("user.home") + "/.kafkagen/config.yml"));
            }

            if (System.getenv("KAFKAGEN_CONFIG") != null) {
                yamlConfigSource = new YamlConfigSource("kafkagen", System.getenv("KAFKAGEN_CONFIG"));
            }

            return Collections.singletonList(yamlConfigSource);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    @Override
    public OptionalInt getPriority() {
        return OptionalInt.of(290);
    }
}
