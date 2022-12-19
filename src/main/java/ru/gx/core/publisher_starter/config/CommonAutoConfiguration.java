package ru.gx.core.publisher_starter.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ConfigurationPropertiesService.class)
@ComponentScan(basePackages = {"ru.gx.core.publisher_starter"})
public class CommonAutoConfiguration {

}