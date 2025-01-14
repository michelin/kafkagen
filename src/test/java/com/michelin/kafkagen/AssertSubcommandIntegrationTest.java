package com.michelin.kafkagen;

import com.michelin.kafkagen.kafka.GenericProducer;
import com.michelin.kafkagen.services.AssertService;
import com.michelin.kafkagen.services.ConfigService;
import com.michelin.kafkagen.services.DatasetService;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import picocli.CommandLine;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class AssertSubcommandIntegrationTest extends AbstractIntegrationTest {

    @InjectMock
    MockedContext context;
    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;
    @Mock
    CommandLine.Model.CommandSpec commandSpec;
    @Mock
    public ConfigService configService;

    @Inject
    public AssertService assertService;
    @Inject
    public DatasetService datasetService;
    @Inject
    public GenericProducer genericProducer;

    private final StringWriter out = new StringWriter();

    @BeforeEach
    public void init() {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(kafkaContext.bootstrapServers()).thenReturn(host + ":" + Integer.parseInt(port));
        Mockito.when(context.definition()).thenReturn(kafkaContext);

        MockitoAnnotations.openMocks(this);
        Mockito.when(configService.getCurrentContextName()).thenReturn("dev");
        Mockito.when(configService.getContextByName(Mockito.anyString())).thenReturn(Optional.of(context));
    }

    @Test
    public void callWithFoundAndNotFoundDatasets() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithoutKey", 3, (short) 1)));
        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetToProduce.json").toURI()), "avroTopicWithoutKey", context);

        genericProducer.produce("avroTopicWithoutKey", dataset, 1, context);

        Thread.sleep(1000);

        var assertSubcommand = new AssertSubcommand(configService, datasetService, assertService);
        initCommandLine(assertSubcommand);
        assertSubcommand.commandSpec = commandSpec;
        assertSubcommand.file = Optional.of(new File(getClass().getClassLoader().getResource("avro/expected/assertDataset.json").toURI()));
        assertSubcommand.topic = Optional.of("avroTopicWithoutKey");
        assertSubcommand.startTimestamp = Optional.empty();

        assertEquals(CommandLine.ExitCode.OK, assertSubcommand.call());
        assertEquals("The dataset has been found in the topic", out.toString().trim());

        assertSubcommand.file = Optional.of(new File(getClass().getClassLoader().getResource("avro/expected/assertDatasetNotFound.json").toURI()));

        assertEquals(CommandLine.ExitCode.USAGE, assertSubcommand.call());
        assertTrue(out.toString().contains("1 records do not match"));
    }

    @Test
    public void callWithoutContext() {
        Mockito.when(configService.getCurrentContextName()).thenReturn(null);
        Mockito.when(configService.getContextByName(Mockito.anyString())).thenReturn(Optional.empty());

        var assertSubcommand = new AssertSubcommand(configService, datasetService, assertService);
        initCommandLine(assertSubcommand);
        assertSubcommand.commandSpec = commandSpec;
        assertSubcommand.file = Optional.empty();

        assertEquals(CommandLine.ExitCode.USAGE, assertSubcommand.call());
        assertEquals("No context selected. Please list/set the context with the config command", out.toString().trim());
    }

    @Test
    public void callWithoutFile() {
        var assertSubcommand = new AssertSubcommand(configService, datasetService, assertService);
        initCommandLine(assertSubcommand);
        assertSubcommand.commandSpec = commandSpec;
        assertSubcommand.file = Optional.empty();

        assertThrows(CommandLine.ParameterException.class, assertSubcommand::call);
    }

    @Test
    public void callWithWrongFile() {
        var assertSubcommand = new AssertSubcommand(configService, datasetService, assertService);
        initCommandLine(assertSubcommand);
        assertSubcommand.commandSpec = commandSpec;
        assertSubcommand.file = Optional.of(new File("wrongFile.yaml"));
        assertSubcommand.topic = Optional.of("topic");

        assertEquals(CommandLine.ExitCode.SOFTWARE, assertSubcommand.call());
        assertEquals("Assert failed due to the following error: Unable to read the provided file wrongFile.yaml. Please check the path and content (text only)", out.toString().trim());
    }

    @Test
    public void callWithoutTopic() {
        var assertSubcommand = new AssertSubcommand(configService, datasetService, assertService);
        initCommandLine(assertSubcommand);
        assertSubcommand.commandSpec = commandSpec;
        assertSubcommand.topic = Optional.empty();
        assertSubcommand.file = Optional.empty();

        assertThrows(CommandLine.ParameterException.class, assertSubcommand::call);
    }

    private void initCommandLine(AssertSubcommand assertSubcommand) {
        var commandLine = new CommandLine(assertSubcommand);
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(out));
        Mockito.when(commandSpec.commandLine()).thenReturn(commandLine);
    }
}
