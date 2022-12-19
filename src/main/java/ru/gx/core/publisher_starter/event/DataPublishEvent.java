package ru.gx.core.publisher_starter.event;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.longtime.LongtimeProcessEvent;
import ru.gx.core.redis.load.PublishSnapshotContext;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
@ToString(callSuper = true)
public class DataPublishEvent extends LongtimeProcessEvent {

    private List<String> channelDescriptorNames;

    private int currentProcessingChannelIndex;

    private int currentPage;

    private boolean isCurrentPageLast;

    private PublishSnapshotContext context;

    public DataPublishEvent(@NotNull final UUID longtimeProcessId) {
        super(longtimeProcessId);
    }
}
