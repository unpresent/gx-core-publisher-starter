package ru.gx.core.publisher_starter.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import ru.gx.core.channels.ChannelApiDescriptor;
import ru.gx.core.channels.ChannelConfigurationException;
import ru.gx.core.data.DataObject;
import ru.gx.core.data.edlinking.AbstractEntitiesUploadingConfiguration;
import ru.gx.core.data.edlinking.EntitiesDtoLinksConfigurationException;
import ru.gx.core.data.edlinking.EntityUploadingDescriptor;
import ru.gx.core.data.entity.EntityObject;
import ru.gx.core.longtime.LongtimeProcess;
import ru.gx.core.longtime.LongtimeProcessService;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.StandardMessagesPrioritizedQueue;
import ru.gx.core.publisher_starter.event.DataPublishEvent;
import ru.gx.core.redis.load.PublishSnapshotContext;
import ru.gx.core.redis.upload.AbstractRedisOutcomeCollectionsConfiguration;
import ru.gx.core.redis.upload.RedisOutcomeCollectionUploadingDescriptor;
import ru.gx.core.redis.upload.RedisOutcomeCollectionsUploader;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static lombok.AccessLevel.PROTECTED;

@Slf4j
@Component
public class DataPublishProcessor {
    @NotNull
    private final AbstractEntitiesUploadingConfiguration entitiesUploadingConfiguration;

    @NotNull
    private final RedisOutcomeCollectionsUploader redisUploader;

    @NotNull
    private final AbstractRedisOutcomeCollectionsConfiguration redisOutcomeTopicsConfiguration;

    @Value("${service.data-publish.processor.batch-size}")
    private int batchSize;

    @NotNull
    private final StandardMessagesPrioritizedQueue standardMessagesPrioritizedQueue;

    @NotNull
    private final LongtimeProcessService longtimeProcessService;

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">

    @Autowired
    public DataPublishProcessor(
            @NotNull final AbstractEntitiesUploadingConfiguration entitiesUploadingConfiguration,
            @NotNull final RedisOutcomeCollectionsUploader redisUploader,
            @NotNull final AbstractRedisOutcomeCollectionsConfiguration redisOutcomeTopicsConfiguration,
            @NotNull StandardMessagesPrioritizedQueue standardMessagesPrioritizedQueue,
            @NotNull LongtimeProcessService longtimeProcessService
    ) {
        this.entitiesUploadingConfiguration = entitiesUploadingConfiguration;
        this.redisUploader = redisUploader;
        this.redisOutcomeTopicsConfiguration = redisOutcomeTopicsConfiguration;
        this.standardMessagesPrioritizedQueue = standardMessagesPrioritizedQueue;
        this.longtimeProcessService = longtimeProcessService;
    }

    public LongtimeProcess startPublishProcess(List<String> channelNames) {
        final var longtimeProcess = this.longtimeProcessService.createLongtimeProcess(null);
        final var dataPublishEvent = new DataPublishEvent(longtimeProcess.getId());
        dataPublishEvent.setChannelDescriptorNames(channelNames);
        this.standardMessagesPrioritizedQueue
                .pushMessage(1, dataPublishEvent);
        return longtimeProcess;
    }

    @EventListener(DataPublishEvent.class)
    public void onEvent(@NotNull final DataPublishEvent event) {
        final var longtimeProcess =
                event.getCurrentProcessingChannelIndex() == 0 ?
                        this.longtimeProcessService.startLongtimeProcess(event.getLongtimeProcessId()) :
                        this.longtimeProcessService.getLongtimeProcess(event.getLongtimeProcessId());
        if (longtimeProcess == null) {
            throw new NullPointerException("LongtimeProcess has not found by id = " + event.getLongtimeProcessId());
        }
        if (event.getCurrentProcessingChannelIndex() == 0) {
            longtimeProcess.setTotal(event.getChannelDescriptorNames().size());
        }
        longtimeProcess.setCurrent(event.getCurrentProcessingChannelIndex() + 1);

        final var channel = event.getChannelDescriptorNames().get(event.getCurrentProcessingChannelIndex());
        final var channelHandlerDescriptor =
                this.redisOutcomeTopicsConfiguration.get(channel);
        log.info("Processing channel: {}, event: {}", channel, event);
        try {
            if (channelHandlerDescriptor.getApi() == null) {
                throw new IllegalStateException("ChannelHandler descriptor is null: " + channelHandlerDescriptor);
            }

            final EntityUploadingDescriptor<? extends ChannelApiDescriptor<? extends Message<? extends MessageBody>>,
                    EntityObject, DataObject> entityUploadingDescriptor =
                    entitiesUploadingConfiguration.getByChannel(channelHandlerDescriptor.getApi());
            final var repository = entityUploadingDescriptor.getRepository();
            final var channelName = entityUploadingDescriptor.getChannelApiDescriptor().getName();
            if (repository == null) {
                throw new EntitiesDtoLinksConfigurationException(
                        "Can't get CrudRepository by ChannelApi "
                                + channelName
                );
            }

            final var converter = entityUploadingDescriptor.getDtoFromEntityConverter();
            if (converter == null) {
                throw new EntitiesDtoLinksConfigurationException(
                        "Can't get Converter by ChannelApi "
                                + channelName
                );
            }

            final var keyExtractor = entityUploadingDescriptor.getKeyExtractor();
            if (keyExtractor == null) {
                throw new EntitiesDtoLinksConfigurationException(
                        "Can't get KeyExtractor by ChannelApi "
                                + channelName
                );
            }

            PublishSnapshotContext context = event.getContext();
            if (event.getContext() == null) {
                context = new PublishSnapshotContext();
                event.setContext(context);
                context.setId(UUID.randomUUID());
                context.setBatchSize(batchSize);
            }

            //вытягиваем страницу
            final var entityObjects = repository.findAll(PageRequest.of(event.getCurrentPage(), batchSize));
            if (entityObjects.getTotalPages() == 0) {
                log.info("No entities found for channel: {}, finishing process.", channelName);
            }
            log.info("Uploading page {} of {} for {}", event.getCurrentPage() + 1, entityObjects.getTotalPages(),
                    channelName);

            // Преобразуем в список DTO
            final var dataObjects = new ArrayList<DataObject>();
            converter.fillDtoCollectionFromSource(dataObjects, entityObjects.getContent());

            // Выгружаем данные
            final var redisDescriptor = getRedisOutcomeDescriptorByChannelApi(
                    entityUploadingDescriptor.getChannelApiDescriptor()
            );
            if (dataObjects.size() > 0) {
                if (!entityObjects.hasNext()) {
                    context.setLast(true);
                }
                this.redisUploader
                        .uploadDataObjects(
                                redisDescriptor,
                                dataObjects,
                                keyExtractor,
                                true,
                                context
                        );
            }

            log.info("Page {} of {} for {} uploaded", event.getCurrentPage() + 1, entityObjects.getTotalPages(),
                    channelName);
            event.setCurrentPageLast(!entityObjects.hasNext());
            if (context.isLast()) {
                context.setLast(false);
            }
            if (!entityObjects.hasNext()) {
                //если это последняя страница, тогда почистим за собой временное множество redis
                if (event.getChannelDescriptorNames().size() > event.getCurrentProcessingChannelIndex() + 1) {
                    //проверяем, есть ли следующий канал
                    event.setCurrentProcessingChannelIndex(event.getCurrentProcessingChannelIndex() + 1);
                    //начинаем обрабатывать следующий канал с нулевой страницы
                    event.setCurrentPage(0);
                }
            } else {
                // обрабатываем этот же канал следующую страницу
                event.setCurrentPage(event.getCurrentPage() + 1);
            }
            if (event.getCurrentProcessingChannelIndex() == event.getChannelDescriptorNames().size() - 1
                    && event.isCurrentPageLast()) {
                // последняя страница, заканчиваем процесс
                this.longtimeProcessService.finishLongtimeProcess(event.getLongtimeProcessId());
            } else {
                // отсылаем следующее событие на выгрузку пакета
                this.standardMessagesPrioritizedQueue.pushMessage(1, event);
            }
        } catch (Exception e) {
            log.error("ERROR due onEvent(DataPublishEvent)", e);
            this.longtimeProcessService.setErrorLongtimeProcess(longtimeProcess.getId(), e.getMessage());
            throw new RuntimeException(e);
        }

    }

    @NotNull
    private RedisOutcomeCollectionUploadingDescriptor
    getRedisOutcomeDescriptorByChannelApi(@NotNull final ChannelApiDescriptor<?> channelApiDescriptor) {
        for (final var descriptor : this.redisOutcomeTopicsConfiguration.getAll()) {
            if (descriptor.getApi() == channelApiDescriptor) {
                return (RedisOutcomeCollectionUploadingDescriptor) descriptor;
            }
        }
        throw new ChannelConfigurationException(
                "There isn't RedisOutcomeCollectionUploadingDescriptor for api: " + channelApiDescriptor.getName()
        );
    }
}