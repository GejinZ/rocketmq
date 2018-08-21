package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryMessageStore {

    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, List<MessageExtBrokerInner>>> messageTable;
    private final Vector<MessageExt> commitLog;
    private final int maxListLength = 1000;

    public InMemoryMessageStore(ConcurrentMap<String, ConcurrentMap<Integer, List<MessageExtBrokerInner>>> message, Vector<MessageExt> commitLog) {
        this.messageTable = message;
        this.commitLog = commitLog;
    }

    private void putConsumeQueue(final String topic, final int queueId, final MessageExtBrokerInner message) {
        ConcurrentMap<Integer/* queueId */, List<MessageExtBrokerInner>> map = this.messageTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, List<MessageExtBrokerInner>>();
            List<MessageExtBrokerInner> tmpList = new ArrayList<>();
            tmpList.add(message);
            map.put(queueId, tmpList);
            this.messageTable.put(topic, map);
        } else {
            map.get(queueId).add(message);
            if (map.get(queueId).size() > this.maxListLength) {
                map.get(queueId).remove(0);
            }
        }

        this.commitLog.add(message);
    }

    public PutMessageResult putMessage(MessageExtBrokerInner msg) {

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();
        this.putConsumeQueue(topic, queueId, msg);

        return null;
    }

    public GetNewMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums) {
        List<MessageExtBrokerInner> messageList = this.messageTable.get(topic).get(queueId);
        if (messageList == null) {
            return null;
        }
        if (offset < messageList.get(0).getQueueOffset() || offset >= messageList.get(messageList.size()-1).getQueueOffset()) {
            return null;
        }

        int startId = 0;
        for (int i = 0; i < messageList.size(); i++) {
            if (offset == messageList.get(i).getQueueOffset()) {
                startId = i;
                break;
            }
        }

        GetNewMessageResult getMessageResult = new GetNewMessageResult();
        for (int i = startId; i < offset + maxMsgNums && i < messageList.size(); i++) {
            getMessageResult.addMessage(messageList.get(i));
        }

        return getMessageResult;
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        List<MessageExtBrokerInner> messageList = this.messageTable.get(topic).get(queueId);
        if (messageList != null) {
            return messageList.get(messageList.size()-1).getQueueOffset();
        }
        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        List<MessageExtBrokerInner> messageList = this.messageTable.get(topic).get(queueId);
        if (messageList != null) {
            return messageList.get(0).getQueueOffset();
        }
        return 0;
    }

    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        GetNewMessageResult getNewMessageResult = getMessage(null, topic, queueId, consumeQueueOffset, 1);
        if (null == getNewMessageResult) {
            return 0;
        }

        MessageExt messageExt = getNewMessageResult.getMessageMapedList().get(0);
        if (null == messageExt) {
            return 0;
        }
        for (int i = 0; i < this.commitLog.size(); i++) {
            if (messageExt.getQueueId() == this.commitLog.get(i).getQueueId()) {
                return i;
            }
        }
        return 0;
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        if (commitLogOffset < 0 || commitLogOffset >= commitLog.size()) {
            return null;
        }
        return commitLog.get((int)commitLogOffset);
    }

    public SelectMappedBufferResult getCommitLogData(long offset) {
        return null;
    }

    public void start() throws Exception { }

    public void shutdown() { }

}
