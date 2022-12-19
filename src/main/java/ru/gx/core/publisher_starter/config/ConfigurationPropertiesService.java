package ru.gx.core.publisher_starter.config;

import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "service.data-publish")
@Getter
@Setter
public class ConfigurationPropertiesService {

    @NestedConfigurationProperty
    private @NotNull Processor processor = new Processor();

    @Getter
    @Setter
    public static class Processor {
        private boolean enabled = false;

        private int batchSize = 1000;

        private String startSchedule;
    }
}

