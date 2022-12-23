package ru.gx.core.publisher_starter.event;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.longtime.LongtimeProcessEvent;
import ru.gx.core.redis.load.PublishSnapshotContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@ToString(callSuper = true)
public class DataPublishEvent extends LongtimeProcessEvent {

    @NotNull
    private final List<String> channelDescriptorNames;

    private int currentProcessingChannelIndex;

    private int currentPage;

    @NotNull
    private final PublishSnapshotContext context;

    public DataPublishEvent(
            @NotNull final UUID longtimeProcessId,
            @NotNull final Collection<String> channelDescriptorNames,
            final int batchSize
    ) {
        super(longtimeProcessId);
        this.channelDescriptorNames = new ArrayList<>();
        this.channelDescriptorNames.addAll(channelDescriptorNames);
        this.context = new PublishSnapshotContext(UUID.randomUUID());
        this.context.setBatchSize(batchSize);
    }

    public String getCurrentChannelName() {
        return getChannelDescriptorNames().get(this.getCurrentProcessingChannelIndex());
    }

}
