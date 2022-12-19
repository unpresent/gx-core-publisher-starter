package ru.gx.core.publisher_starter.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;
import ru.gx.core.longtime.LongtimeProcess;
import ru.gx.core.publisher_starter.service.DataPublishService;

import java.util.List;

@SuppressWarnings("unused")
@Tag(name = "Выгрузчик в Redis", description = "Сервис триггер внеочередной выгрузки snapshot-ов в Redis")
@RestController
@RequestMapping("/snapshot-uploader")
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(value = "service.data-publish.processor.enabled", havingValue = "true")
public class DataPublishController {

    private final DataPublishService dataPublishService;

    @PostMapping(path = "/upload-all")
    @Operation(summary = "Выгрузка всех справочников в Redis")
    public LongtimeProcess doUploadAll() {
        final var started = System.currentTimeMillis();
        log.info("START /upload-all");
        return dataPublishService.publishAll();
    }

    @PostMapping(path = "/upload-selected")
    public LongtimeProcess doUploadSelected(@RequestBody List<String> channelNames) {
        log.info("START /upload-selected: {}", channelNames);

        return dataPublishService.publish(channelNames);
    }

    @GetMapping(path = "/get-available-channels")
    public @ResponseBody List<String> getAvailableChannels() {
        return dataPublishService.getAvailableChannels();
    }
}
