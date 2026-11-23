package org.pranay.aqueduct.launch;

import org.pranay.aqueduct.consumer.AqueductConsumer;
import org.pranay.aqueduct.consumer.AqueductConsumerFactory;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.depot.config.MetricsConfig;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.depot.metrics.StatsDReporterBuilder;
import org.pranay.aqueduct.config.KafkaConsumerConfig;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;

/**
 * Main class to run aqueduct.
 */
public class Main {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws InterruptedException the interrupted exception
     */
    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerConfig kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, System.getenv());
        multiThreadedConsumers(kafkaConsumerConfig);
    }

    private static void multiThreadedConsumers(KafkaConsumerConfig kafkaConsumerConfig) throws InterruptedException {
        MetricsConfig config = ConfigFactory.create(MetricsConfig.class, System.getenv());
        StatsDReporter statsDReporter = StatsDReporterBuilder.builder().withMetricConfig(config)
                .withExtraTags(Metrics.tag(Metrics.CONSUMER_GROUP_ID_TAG, kafkaConsumerConfig.getSourceKafkaConsumerGroupId()))
                .build();
        AqueductInstrumentation aqueductInstrumentation = new AqueductInstrumentation(statsDReporter, Main.class);
        aqueductInstrumentation.logInfo("Number of consumer threads: " + kafkaConsumerConfig.getApplicationThreadCount());
        aqueductInstrumentation.logInfo("Delay to clean up consumer threads in ms: " + kafkaConsumerConfig.getApplicationThreadCleanupDelay());

        Task consumerTask = new Task(
                kafkaConsumerConfig.getApplicationThreadCount(),
                kafkaConsumerConfig.getApplicationThreadCleanupDelay(),
                new AqueductInstrumentation(statsDReporter, Task.class),
                taskFinished -> {

                    AqueductConsumer aqueductConsumer = null;
                    try {
                        aqueductConsumer = new AqueductConsumerFactory(kafkaConsumerConfig, statsDReporter).buildConsumer();
                        while (true) {
                            if (Thread.interrupted()) {
                                aqueductInstrumentation.logWarn("Consumer Thread interrupted, leaving the loop!");
                                break;
                            }
                            aqueductConsumer.process();
                        }
                    } catch (Exception | Error e) {
                        ensureThreadInterruptStateIsClearedAndClose(aqueductConsumer, aqueductInstrumentation);
                        aqueductInstrumentation.captureFatalError("aqueduct_error_event", e, "Caught exception or error, exiting the application");
                        System.exit(1);
                    } finally {
                        ensureThreadInterruptStateIsClearedAndClose(aqueductConsumer, aqueductInstrumentation);
                        taskFinished.run();
                    }
                });
        aqueductInstrumentation.logInfo("Consumer Task Created");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            aqueductInstrumentation.logInfo("Program is going to exit. Have started execution of shutdownHook before this");
            consumerTask.stop();
        }));

        consumerTask.run().waitForCompletion();
        aqueductInstrumentation.logInfo("Exiting main thread");
    }

    private static void ensureThreadInterruptStateIsClearedAndClose(AqueductConsumer aqueductConsumer, AqueductInstrumentation aqueductInstrumentation) {
        try {
            if (aqueductConsumer != null) {
                aqueductConsumer.close();
            }
        } catch (IOException e) {
            aqueductInstrumentation.captureFatalError("aqueduct_error_event", e, "Exception on closing aqueduct consumer");
        }
    }
}
