package com.michelin.kafkagen.services;

import com.michelin.kafkagen.AbstractIntegrationTest;
import com.michelin.kafkagen.KafkaTestResource;
import com.michelin.kafkagen.kafka.GenericProducer;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class AssertServiceIntegrationTest extends AbstractIntegrationTest {

    @InjectMock
    MockedContext context;

    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;

    @Inject
    DatasetService datasetService;

    @Inject
    AssertService assertService;

    @Inject
    GenericProducer genericProducer;

    @BeforeEach
    public void init() {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(kafkaContext.bootstrapServers()).thenReturn(host + ":" + Integer.parseInt(port));
        Mockito.when(context.definition()).thenReturn(kafkaContext);
    }

    @Test
    public void assertThatTopicContains() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithoutKey", 3, (short) 1)));
        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetToProduce.json").toURI()), "avroTopicWithoutKey", context);

        Thread.sleep(1000);

        genericProducer.produce("avroTopicWithoutKey", dataset, 1, context);

        Thread.sleep(1000);

        var rawDataset = datasetService.getRawRecord(new File(getClass().getClassLoader().getResource("avro/expected/assertDataset.json").toURI()));
        boolean result = assertService.assertThatTopicContains("avroTopicWithoutKey", rawDataset, Map.of(), context, false, Optional.empty());
        assertTrue(result);

        result = assertService.assertThatTopicContains("avroTopicWithoutKey", rawDataset, Map.of(), context, false, Optional.of(646264800000L));
        assertTrue(result);

        result = assertService.assertThatTopicContains("avroTopicWithoutKey", rawDataset, Map.of(), context, false, Optional.of(1718865689000L));
        assertFalse(result);

        rawDataset = datasetService.getRawRecord(new File(getClass().getClassLoader().getResource("avro/expected/assertDatasetNotFound.json").toURI()));
        result = assertService.assertThatTopicContains("avroTopicWithoutKey", rawDataset, Map.of(), context, false, Optional.empty());
        assertFalse(result);
    }
}
