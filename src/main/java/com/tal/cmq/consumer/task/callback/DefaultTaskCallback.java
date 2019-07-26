package com.tal.cmq.consumer.task.callback;

import com.qcloud.cmq.client.consumer.Consumer;
import com.qcloud.cmq.client.consumer.Message;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author ZhouJie
 * @Date 2019/7/25 20:19
 * @Description 默认任务回调
 */
public class DefaultTaskCallback implements TaskCallback {
    private Consumer consumer;
    private String queueName;
    private AtomicInteger messageCount;

    public DefaultTaskCallback(Consumer consumer, String queueName, AtomicInteger messageCount) {
        this.consumer = consumer;
        this.queueName = queueName;
        this.messageCount = messageCount;
    }

    @Override
    public void onSuccess(Message message) {
        try {
            consumer.deleteMsg(queueName, message.getReceiptHandle());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onException(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void onFinally() {
        messageCount.decrementAndGet();
    }
}
