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
