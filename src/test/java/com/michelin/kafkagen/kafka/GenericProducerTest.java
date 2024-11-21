package com.michelin.kafkagen.kafka;

import com.michelin.kafkagen.AbstractIntegrationTest;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.NewTopic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

@QuarkusTest
public class GenericProducerTest extends AbstractIntegrationTest {
    @Inject
    GenericProducer genericProducer;

    @InjectMock
    MockedContext context;
    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;

    //@BeforeEach
    public void init() {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(context.definition()).thenReturn(kafkaContext);

        getAdminClient().createTopics(List.of(new NewTopic("topic", 1, (short) 1)));
    }
}
