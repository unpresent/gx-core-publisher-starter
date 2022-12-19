package ru.gx.core.publisher_starter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import ru.gx.core.data.edlinking.AbstractEntitiesUploadingConfiguration;
import ru.gx.core.longtime.LongtimeProcess;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(value = "service.data-publish.processor.enabled", havingValue = "true")
public class DataPublishService {

    @NotNull
    private final DataPublishProcessor publishProcessor;

    @NotNull
    private final AbstractEntitiesUploadingConfiguration entitiesUploadingConfiguration;

    public LongtimeProcess publishAll() {
        final var channelNames = new ArrayList<String>();
        this.entitiesUploadingConfiguration
                .getAll()
                .forEach(
                        ch -> channelNames.add(ch.getChannelApiDescriptor().getName())
                );
        return this.publishProcessor.startPublishProcess(channelNames);
    }

    public LongtimeProcess publish(List<String> channelNames) {
        if (channelNames == null || channelNames.isEmpty()) {
            throw new IllegalStateException("Channel names must not be empty");
        }
        final var availableChannels = getAvailableChannels();
        for (final var channelName : channelNames) {
            if (!availableChannels.contains(channelName)) {
                throw new IllegalStateException("Channel not found: " + channelName);
            }
        }
        return this.publishProcessor.startPublishProcess(channelNames);
    }

    public List<String> getAvailableChannels() {
        final List<String> result = new ArrayList<>();
        this.entitiesUploadingConfiguration
                .getAll()
                .forEach(
                        c -> result.add(c.getChannelApiDescriptor().getName())
                );
        return result;
    }
}