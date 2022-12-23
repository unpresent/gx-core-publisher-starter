package ru.gx.core.publisher_starter.service;

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
import ru.gx.core.messaging.MessagesPrioritizedQueue;
import ru.gx.core.publisher_starter.event.DataPublishEvent;
import ru.gx.core.redis.load.PublishSnapshotContext;
import ru.gx.core.redis.upload.AbstractRedisOutcomeCollectionsConfiguration;
import ru.gx.core.redis.upload.RedisOutcomeCollectionUploadingDescriptor;
import ru.gx.core.redis.upload.RedisOutcomeCollectionsUploader;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
    private final MessagesPrioritizedQueue messagesPrioritizedQueue;

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
            @NotNull final MessagesPrioritizedQueue messagesPrioritizedQueue,
            @NotNull final LongtimeProcessService longtimeProcessService
    ) {
        this.entitiesUploadingConfiguration = entitiesUploadingConfiguration;
        this.redisUploader = redisUploader;
        this.redisOutcomeTopicsConfiguration = redisOutcomeTopicsConfiguration;
        this.messagesPrioritizedQueue = messagesPrioritizedQueue;
        this.longtimeProcessService = longtimeProcessService;
    }

    public LongtimeProcess startPublishProcess(List<String> channelNames) {
        final var longtimeProcess = this.longtimeProcessService.createLongtimeProcess(null);
        final var dataPublishEvent = new DataPublishEvent(longtimeProcess.getId(), channelNames, this.batchSize);
        this.messagesPrioritizedQueue.pushMessage(1, dataPublishEvent);
        return longtimeProcess;
    }

    @EventListener(DataPublishEvent.class)
    public void onDataPublishEvent(@NotNull final DataPublishEvent event) {
        final var longtimeProcess =
                event.getCurrentProcessingChannelIndex() == 0 ?
                        this.longtimeProcessService.startLongtimeProcess(event.getLongtimeProcessId()) :
                        this.longtimeProcessService.getLongtimeProcess(event.getLongtimeProcessId());
        if (longtimeProcess == null) {
            throw new NullPointerException("LongtimeProcess has not found by id = " + event.getLongtimeProcessId() + "!");
        }
        if (event.getCurrentProcessingChannelIndex() == 0) {
            longtimeProcess.setTotal(event.getChannelDescriptorNames().size());
        }
        longtimeProcess.setCurrent(event.getCurrentProcessingChannelIndex() + 1);

        final var channelName = event.getCurrentChannelName();
        final var channelHandlerDescriptor =
                this.redisOutcomeTopicsConfiguration.get(channelName);
        final var context = event.getContext();

        log.info(
                "/onDataPublishEvent()/longProcessId: {}/channel: {}/ Before processing channelIndex: {}, page: {}, context.batchSize: {}",
                longtimeProcess.getId(),
                channelName,
                event.getCurrentProcessingChannelIndex(),
                event.getCurrentPage(),
                context.getBatchSize()
        );

        try {
            if (channelHandlerDescriptor.getApi() == null) {
                throw new IllegalStateException("ChannelHandler descriptor is null: " + channelHandlerDescriptor + "!");
            }

            final var entityUploadingDescriptor =
                    this.entitiesUploadingConfiguration.getByChannel(channelHandlerDescriptor.getApi());
            final var repository = entityUploadingDescriptor.getRepository();
            final var channelApiName = entityUploadingDescriptor.getChannelApiDescriptor().getName();
            if (repository == null) {
                throw new EntitiesDtoLinksConfigurationException(
                        "/onDataPublishEvent()/longProcessId: " + longtimeProcess.getId() + "/channel: " + channelApiName + "/ Can't get CrudRepository by ChannelApi!"
                );
            }

            final var converter = entityUploadingDescriptor.getDtoFromEntityConverter();
            if (converter == null) {
                throw new EntitiesDtoLinksConfigurationException(
                        "/onDataPublishEvent()/longProcessId: " + longtimeProcess.getId() + "/channel: " + channelApiName + "/ Can't get Converter by ChannelApi!"
                );
            }

            final var keyExtractor = entityUploadingDescriptor.getKeyExtractor();
            if (keyExtractor == null) {
                throw new EntitiesDtoLinksConfigurationException(
                        "/onDataPublishEvent()/longProcessId: " + longtimeProcess.getId()
                                + "/channel: " + channelApiName
                                + "/ Can't get KeyExtractor by ChannelApi!"
                );
            }

            // вытягиваем страницу
            var debugDateTime = System.currentTimeMillis();
            final var entityObjects = repository
                    .findAll(PageRequest.of(event.getCurrentPage(), batchSize));
            log.info(
                    "/onDataPublishEvent()/longProcessId: {}/channel: {}/ Loaded from DB {} rows in page {} of {} in {} ms",
                    longtimeProcess.getId(),
                    channelApiName,
                    entityObjects.getSize(),
                    event.getCurrentPage(),
                    entityObjects.getTotalPages(),
                    System.currentTimeMillis() - debugDateTime
            );
            if (entityObjects.getTotalPages() == 0) {
                log.info(
                        "/onDataPublishEvent()/longProcessId: {}/channel: {}/ No entities found, finishing process.",
                        longtimeProcess.getId(),
                        channelApiName
                );
            }

            // Преобразуем в список DTO
            debugDateTime = System.currentTimeMillis();
            final var dataObjects = new ArrayList<DataObject>();
            converter.fillDtoCollectionFromSource(dataObjects, entityObjects.getContent());
            log.info(
                    "/onDataPublishEvent()/longProcessId: {}/channel: {}/ Converted to DTO {} rows in {} ms",
                    longtimeProcess.getId(),
                    channelApiName,
                    dataObjects.size(),
                    System.currentTimeMillis() - debugDateTime
            );

            // Выгружаем данные
            debugDateTime = System.currentTimeMillis();
            final var redisDescriptor = getRedisOutcomeDescriptorByChannelApi(
                    entityUploadingDescriptor.getChannelApiDescriptor()
            );
            if (dataObjects.size() > 0) {
                if (!entityObjects.hasNext()) {
                    log.info(
                            "/onDataPublishEvent()/longProcessId: {}/channel: {}/ context.setLast(true)",
                            longtimeProcess.getId(),
                            channelApiName
                    );
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
                log.info(
                        "/onDataPublishEvent()/longProcessId: {}/channel: {}/ Uploaded into Redis {} rows in page {} of {} in {} ms",
                        longtimeProcess.getId(),
                        channelApiName,
                        dataObjects.size(),
                        event.getCurrentPage(),
                        entityObjects.getTotalPages(),
                        System.currentTimeMillis() - debugDateTime
                );
            }

            var isFinish = false;
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
                } else {
                    isFinish = true;
                }
            } else {
                // обрабатываем этот же канал следующую страницу
                event.setCurrentPage(event.getCurrentPage() + 1);
            }
            if (isFinish) {
                log.info(
                        "/onDataPublishEvent()/longProcessId: {}/channel: {}/ CALL finishLongtimeProcess()",
                        longtimeProcess.getId(),
                        channelApiName
                );

                // последняя страница, заканчиваем процесс
                this.longtimeProcessService.finishLongtimeProcess(event.getLongtimeProcessId());
            } else {
                // отсылаем следующее событие на выгрузку пакета
                this.messagesPrioritizedQueue.pushMessage(1, event);
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