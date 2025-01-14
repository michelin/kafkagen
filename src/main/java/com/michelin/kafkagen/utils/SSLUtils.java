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

/**
 * Utility class to turn off SSL for Kafka clients and schema registry.
 */
@Slf4j
@RegisterForReflection(targets = {CachedSchemaRegistryClient.class})
public class SSLUtils {

    /**
     * Turn off SSL for the schema registry client.
     *
     * @param schemaRegistryClient The schema registry client
     */
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
            log.error("Error turning schema registry client insecure", e);
        }
    }

    /**
     * Turn off SSL for the Kafka client.
     *
     * @param kafkaClient The Kafka client
     * @param insecureSchemaRegistryClient The insecure schema registry client
     */
    public static void turnKafkaClientInsecure(Object kafkaClient, SchemaRegistryClient insecureSchemaRegistryClient) {
        try {
            setInsecureRegistryClientToKafkaClient(kafkaClient.getClass().getDeclaredField("keySerializer"),
                kafkaClient, insecureSchemaRegistryClient);
            setInsecureRegistryClientToKafkaClient(kafkaClient.getClass().getDeclaredField("valueSerializer"),
                kafkaClient, insecureSchemaRegistryClient);
        } catch (NoSuchFieldException e) {
            log.error("Error turning Kafka client insecure", e);
        }
    }

    /**
     * Set the insecure schema registry client to the Kafka client.
     *
     * @param serDeField The serializer/deserializer field
     * @param kafkaClient The Kafka client
     * @param insecureSchemaRegistryClient The insecure schema registry client
     */
    private static void setInsecureRegistryClientToKafkaClient(Field serDeField, Object kafkaClient,
                                                               SchemaRegistryClient insecureSchemaRegistryClient) {
        try {
            serDeField.setAccessible(true);
            Object keySerializerObject = serDeField.get(kafkaClient);

            if (keySerializerObject instanceof AbstractKafkaSchemaSerDe) {
                Field valueRegistryClientFiel = AbstractKafkaSchemaSerDe.class.getDeclaredField("schemaRegistry");
                valueRegistryClientFiel.setAccessible(true);
                valueRegistryClientFiel.set(keySerializerObject, insecureSchemaRegistryClient);
            }
        } catch (Exception e) {
            log.error("Error setting insecure schema registry client to Kafka client", e);
        }
    }
}
