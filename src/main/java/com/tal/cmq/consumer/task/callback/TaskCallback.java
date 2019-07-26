package com.tal.cmq.consumer.task.callback;

import com.qcloud.cmq.client.consumer.Message;

/**
 * @Author ZhouJie
 * @Date 2019/7/25 20:18
 * @Description 任务回调接口
 */
public interface TaskCallback {
    void onSuccess(Message message);
    void onException(Throwable throwable);
    void onFinally();
}
