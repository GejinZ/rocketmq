package org.apache.rocketmq.store;

import java.util.ArrayList;
import java.util.List;

public class GetNewMessageResult {
    public List<MessageExtBrokerInner> getMessageMapedList() {
        return messageMapedList;
    }

    private final List<MessageExtBrokerInner> messageMapedList =
            new ArrayList<>(100);
    private long minOffset;
    private long maxOffset;

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public void addMessage(final MessageExtBrokerInner message) {
        this.messageMapedList.add(message);
    }
}
