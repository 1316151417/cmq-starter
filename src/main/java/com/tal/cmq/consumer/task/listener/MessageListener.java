package com.tal.cmq.consumer.task.listener;

import com.qcloud.cmq.client.consumer.Message;

/**
 * @Author ZhouJie
 * @Date 2019/7/25 20:18
 * @Description 消息监听器接口
 */
public interface MessageListener {
    void consume(Message message) throws Exception;
}
