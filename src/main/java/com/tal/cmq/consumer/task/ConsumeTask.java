package com.tal.cmq.consumer.task;

import com.qcloud.cmq.client.consumer.Message;
import com.tal.cmq.consumer.task.callback.TaskCallback;
import com.tal.cmq.consumer.task.listener.MessageListener;

/**
 * @Author ZhouJie
 * @Date 2019/7/25 20:18
 * @Description 消费任务
 */
public class ConsumeTask implements Runnable {

    private Message message;
    private MessageListener messageListener;
    private TaskCallback taskCallback;

    public ConsumeTask(Message message, MessageListener messageListener, TaskCallback taskCallback) {
        this.message = message;
        this.messageListener = messageListener;
        this.taskCallback = taskCallback;
    }

    @Override
    public void run() {
        try {
            messageListener.consume(message);
            taskCallback.onSuccess(message);
        } catch (Exception e) {
            taskCallback.onException(e);
        } finally {
            taskCallback.onFinally();
        }
    }
}
