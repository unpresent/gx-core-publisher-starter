package ru.gx.core.publisher_starter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(value = "service.data-publish.processor.enabled", havingValue = "true")
public class DataPublishScheduler {

    private final DataPublishService dataPublishService;

    @Scheduled(cron = "${service.data-publish.processor.start-schedule}")
    private void publishAllOnSchedule() {
        log.info("CALL publishAllOnSchedule()");
        dataPublishService.publishAll();
    }

    @PostConstruct
    private void firstStart() {
        log.info("CALL initial publishAll()");
        dataPublishService.publishAll();
    }

}
