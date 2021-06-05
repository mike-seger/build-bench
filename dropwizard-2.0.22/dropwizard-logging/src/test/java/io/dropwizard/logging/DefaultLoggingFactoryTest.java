package io.dropwizard.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.spi.LifeCycle;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.logging.filter.FilterFactory;
import io.dropwizard.util.Lists;
import io.dropwizard.util.Maps;
import io.dropwizard.util.Resources;
import io.dropwizard.validation.BaseValidator;
import org.apache.commons.text.StringSubstitutor;
import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultLoggingFactoryTest {
    private final ObjectMapper objectMapper = Jackson.newObjectMapper();
    private final YamlConfigurationFactory<DefaultLoggingFactory> factory = new YamlConfigurationFactory<>(
            DefaultLoggingFactory.class,
            BaseValidator.newValidator(),
            objectMapper, "dw");

    private DefaultLoggingFactory config;

    @BeforeEach
    void setUp() throws Exception {
        objectMapper.getSubtypeResolver().registerSubtypes(ConsoleAppenderFactory.class,
                FileAppenderFactory.class,
                SyslogAppenderFactory.class);

        config = factory.build(new File(Resources.getResource("yaml/logging.yml").toURI()));
    }

    @Test
    void hasADefaultLevel() {
        assertThat(config.getLevel()).isEqualTo("INFO");
    }

    @Test
    void loggerLevelsCanBeOff() throws Exception {
        DefaultLoggingFactory config = null;
        try {
            config = factory.build(new File(Resources.getResource("yaml/logging_level_off.yml").toURI()));
            config.configure(new MetricRegistry(), "test-logger");

            final ILoggerFactory loggerContext = LoggerFactory.getILoggerFactory();
            final Logger rootLogger = ((LoggerContext) loggerContext).getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
            final Logger appLogger = ((LoggerContext) loggerContext).getLogger("com.example.app");
            final Logger newAppLogger = ((LoggerContext) loggerContext).getLogger("com.example.newApp");
            final Logger legacyAppLogger = ((LoggerContext) loggerContext).getLogger("com.example.legacyApp");

            assertThat(rootLogger.getLevel()).isEqualTo(Level.OFF);
            assertThat(appLogger.getLevel()).isEqualTo(Level.OFF);
            assertThat(newAppLogger.getLevel()).isEqualTo(Level.OFF);
            assertThat(legacyAppLogger.getLevel()).isEqualTo(Level.OFF);
        } finally {
            if (config != null) {
                config.reset();
            }
        }
    }

    @Test
    void canParseNewLoggerFormat() throws Exception {
        final DefaultLoggingFactory config = factory.build(
                new File(Resources.getResource("yaml/logging_advanced.yml").toURI()));

        assertThat(config.getLoggers()).contains(MapEntry.entry("com.example.app", new TextNode("INFO")));

        final JsonNode newApp = config.getLoggers().get("com.example.newApp");
        assertThat(newApp).isNotNull();
        final LoggerConfiguration newAppConfiguration = objectMapper.treeToValue(newApp, LoggerConfiguration.class);
        assertThat(newAppConfiguration.getLevel()).isEqualTo("DEBUG");
        assertThat(newAppConfiguration.getAppenders()).hasSize(1);
        final AppenderFactory<ILoggingEvent> appenderFactory = newAppConfiguration.getAppenders().get(0);
        assertThat(appenderFactory).isInstanceOf(FileAppenderFactory.class);
        final FileAppenderFactory<ILoggingEvent> fileAppenderFactory = (FileAppenderFactory<ILoggingEvent>) appenderFactory;
        assertThat(fileAppenderFactory.getCurrentLogFilename()).isEqualTo("${new_app}.log");
        assertThat(fileAppenderFactory.getArchivedLogFilenamePattern()).isEqualTo("${new_app}-%d.log.gz");
        assertThat(fileAppenderFactory.getArchivedFileCount()).isEqualTo(5);
        assertThat(fileAppenderFactory.getBufferSize().toKibibytes()).isEqualTo(256);
        final List<FilterFactory<ILoggingEvent>> filterFactories = fileAppenderFactory.getFilterFactories();
        assertThat(filterFactories).hasSize(2);
        assertThat(filterFactories.get(0)).isExactlyInstanceOf(TestFilterFactory.class);
        assertThat(filterFactories.get(1)).isExactlyInstanceOf(SecondTestFilterFactory.class);

        final JsonNode legacyApp = config.getLoggers().get("com.example.legacyApp");
        assertThat(legacyApp).isNotNull();
        final LoggerConfiguration legacyAppConfiguration = objectMapper.treeToValue(legacyApp, LoggerConfiguration.class);
        assertThat(legacyAppConfiguration.getLevel()).isEqualTo("DEBUG");
        // We should not create additional appenders, if they are not specified
        assertThat(legacyAppConfiguration.getAppenders()).isEmpty();
    }

    @Test
    void testConfigure(@TempDir Path tempDir) throws Exception {
        final StringSubstitutor substitutor = new StringSubstitutor(Maps.of(
                "new_app", tempDir.resolve("example-new-app").toFile().getAbsolutePath(),
                "new_app_not_additive", tempDir.resolve("example-new-app-not-additive").toFile().getAbsolutePath(),
                "default", tempDir.resolve("example").toFile().getAbsolutePath()
        ));

        final String configPath = Resources.getResource("yaml/logging_advanced.yml").getFile();
        DefaultLoggingFactory config = null;
        try {
            config = factory.build(new SubstitutingSourceProvider(new FileConfigurationSourceProvider(), substitutor), configPath);
            config.configure(new MetricRegistry(), "test-logger");

            LoggerFactory.getLogger("com.example.app").debug("Application debug log");
            LoggerFactory.getLogger("com.example.app").info("Application log");
            LoggerFactory.getLogger("com.example.newApp").debug("New application debug log");
            LoggerFactory.getLogger("com.example.newApp").info("New application info log");
            LoggerFactory.getLogger("com.example.legacyApp").debug("Legacy application debug log");
            LoggerFactory.getLogger("com.example.legacyApp").info("Legacy application info log");
            LoggerFactory.getLogger("com.example.notAdditive").debug("Not additive application debug log");
            LoggerFactory.getLogger("com.example.notAdditive").info("Not additive application info log");

            config.stop();
            config.reset();

            assertThat(Files.readAllLines(tempDir.resolve("example.log"))).containsOnly(
                    "INFO  com.example.app: Application log",
                    "DEBUG com.example.newApp: New application debug log",
                    "INFO  com.example.newApp: New application info log",
                    "DEBUG com.example.legacyApp: Legacy application debug log",
                    "INFO  com.example.legacyApp: Legacy application info log");

            assertThat(Files.readAllLines(tempDir.resolve("example-new-app.log"))).containsOnly(
                    "DEBUG com.example.newApp: New application debug log",
                    "INFO  com.example.newApp: New application info log");

            assertThat(Files.readAllLines(tempDir.resolve("example-new-app-not-additive.log"))).containsOnly(
                    "DEBUG com.example.notAdditive: Not additive application debug log",
                    "INFO  com.example.notAdditive: Not additive application info log");
        } finally {
            if (config != null) {
                config.reset();
            }
        }
    }

    @Test
    void testResetAppenders() throws Exception {
        final String configPath = Resources.getResource("yaml/logging.yml").getFile();
        final DefaultLoggingFactory config = factory.build(new FileConfigurationSourceProvider(), configPath);
        config.configure(new MetricRegistry(), "test-logger");

        config.reset();

        // There should be exactly one appender configured, a ConsoleAppender
        final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        final List<Appender<ILoggingEvent>> appenders = Lists.of(logger.iteratorForAppenders());
        assertThat(appenders).hasAtLeastOneElementOfType(ConsoleAppender.class);
        assertThat(appenders).as("context").allMatch((Appender<?> a) -> a.getContext() != null);
        assertThat(appenders).as("started").allMatch(LifeCycle::isStarted);
        assertThat(appenders).hasSize(1);
    }

    @Test
    void testToStringIsImplemented() {
        assertThat(config.toString()).startsWith(
                "DefaultLoggingFactory{level=INFO, loggers={com.example.app=\"DEBUG\"}, appenders=");
    }
}
