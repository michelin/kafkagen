package com.michelin.kafkagen.utils;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.lang.reflect.Field;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RegisterForReflection(targets = {CachedSchemaRegistryClient.class})
public class SSLUtils {

    public static void turnSchemaRegistryClientInsecure(SchemaRegistryClient schemaRegistryClient) {
        // Disable SSL for the Kafka cluster connection
        try {
            TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            }
            };

            SSLContext sc = SSLContext.getInstance("TLSv1.3");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());

            Field field = schemaRegistryClient.getClass().getDeclaredField("restService");
            field.setAccessible(true);
            RestService value = (RestService) field.get(schemaRegistryClient);
            value.setSslSocketFactory(sc.getSocketFactory());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void turnKafkaClientInsecure(Object kafkaClient, SchemaRegistryClient insecureSchemaRegistryClient) {
        try {
            setInsecureRegistryClientToKafkaClient(kafkaClient.getClass().getDeclaredField("keySerializer"),
                kafkaClient, insecureSchemaRegistryClient);
            setInsecureRegistryClientToKafkaClient(kafkaClient.getClass().getDeclaredField("valueSerializer"),
                kafkaClient, insecureSchemaRegistryClient);
        } catch (Exception e) {

        }
    }

    private static void setInsecureRegistryClientToKafkaClient(Field serDeField, Object kafkaClient, SchemaRegistryClient insecureSchemaRegistryClient)
        throws NoSuchFieldException, IllegalAccessException {

        serDeField.setAccessible(true);
        Object keySerializerObject = serDeField.get(kafkaClient);

        if (keySerializerObject instanceof AbstractKafkaSchemaSerDe) {
            Field valueRegistryClientFiel = AbstractKafkaSchemaSerDe.class.getDeclaredField("schemaRegistry");
            valueRegistryClientFiel.setAccessible(true);
            valueRegistryClientFiel.set(keySerializerObject, insecureSchemaRegistryClient);
        }
    }
}
