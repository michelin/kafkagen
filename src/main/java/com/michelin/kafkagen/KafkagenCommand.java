package com.michelin.kafkagen;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@TopCommand
@Command(name = "kafkagen",
        subcommands = {
            ProduceSubcommand.class,
            PlaySubcommand.class,
            SampleSubcommand.class,
            DatasetSubcommand.class,
            AssertSubcommand.class,
            ConfigSubcommand.class
        },
        mixinStandardHelpOptions = true)
public class KafkagenCommand implements Callable<Integer> {

    public Integer call() {
        var cmd = new CommandLine(new KafkagenCommand());
        cmd.usage(System.out);
        return 0;
    }
}
