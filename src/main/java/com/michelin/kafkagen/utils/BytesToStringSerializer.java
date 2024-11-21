package com.michelin.kafkagen.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * This is a custom Jackson serializer that converts byte arrays into strings.
 * It's useful when you're serializing data to JSON and you want to represent
 * byte arrays as plain strings instead of Base64-encoded strings.
 */
public class BytesToStringSerializer extends StdSerializer<byte[]> {

    public BytesToStringSerializer() {
        super(byte[].class);
    }

    protected BytesToStringSerializer(Class<byte[]> t) {
        super(t);
    }

    @Override
    public void serialize(byte[] value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        String readableString = new String(value, StandardCharsets.UTF_8);
        gen.writeString(readableString);
    }
}
