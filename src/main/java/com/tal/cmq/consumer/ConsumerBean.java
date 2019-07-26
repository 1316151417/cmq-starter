package com.tal.cmq.consumer;

import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.consumer.Consumer;
import com.qcloud.cmq.client.consumer.ReceiveResult;
import com.tal.cmq.consumer.task.ConsumeTask;
import com.tal.cmq.consumer.task.callback.DefaultTaskCallback;
import com.tal.cmq.consumer.task.callback.TaskCallback;
import com.tal.cmq.consumer.task.listener.MessageListener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author ZhouJie
 * @Date 2019/7/25 20:10
 * @Description 消费实例
 */
public class ConsumerBean {
    /**
     * 默认值
     */
    /**
     * 默认拉取消息线程池大小
     */
    private static final Integer DEFAULT_RECEIVE_THREAD_POOL_SIZE = 1;
    /**
     * 默认消费消息线程池大小
     */
    private static final Integer DEFAULT_CONSUME_THREAD_POOL_SIZE = 16;



    /**
     * 可配置参数
     */
    /**
     * cmq消费者
     */
    private Consumer consumer;
    /**
     * 队列名
     */
    private String queueName;
    /**
     * 消息监听器
     */
    private MessageListener messageListener;
    /**
     * 拉取消息线程池大小
     */
    private Integer receiveThreadPoolSize;
    /**
     * 消费消息线程池大小
     */
    private Integer consumeThreadPoolSize;



    /**
     * 无参构造器
     */
    public ConsumerBean() {
    }
    /**
     * 带参构造器
     * @param consumer 消费者
     * @param queueName 队列名称
     * @param messageListener 消息监听器
     * @param receiveThreadPoolSize 拉取消息线程池大小
     * @param consumeThreadPoolSize 消费消息线程池大小
     */
    public ConsumerBean(Consumer consumer, String queueName, MessageListener messageListener, Integer receiveThreadPoolSize, Integer consumeThreadPoolSize) {
        this.consumer = consumer;
        this.queueName = queueName;
        this.messageListener = messageListener;
        this.receiveThreadPoolSize = receiveThreadPoolSize;
        this.consumeThreadPoolSize = consumeThreadPoolSize;
    }



    /**
     * 内部参数
     */
    /**
     * 启动状态
     */
    private AtomicBoolean state;
    /**
     * 消息堆积数量
     */
    private AtomicInteger messageCount;
    /**
     * 消费任务回调（用于删除消息、维护消息堆积数量）
     */
    private TaskCallback taskCallback;
    /**
     * 拉取消息线程池
     */
    private ExecutorService receivethreadPoolExecutor;
    /**
     * 消费消息线程池
     */
    private ExecutorService consumethreadPoolExecutor;



    /**
     * 启动
     */
    public void start() {
        init();
        consume();
    }


    /**
     * 关闭
     */
    public void shutdown() {
        state.set(false);
        receivethreadPoolExecutor.shutdown();
        consumethreadPoolExecutor.shutdown();
    }



    /**
     * 初始化
     */
    private void init() {
        if (consumer == null || queueName == null || messageListener == null) {
            throw new NullPointerException();
        }
        /**
         * 可配置参数初始化
         */
        receiveThreadPoolSize = receiveThreadPoolSize == null ? DEFAULT_RECEIVE_THREAD_POOL_SIZE : receiveThreadPoolSize;
        consumeThreadPoolSize = consumeThreadPoolSize == null ? DEFAULT_CONSUME_THREAD_POOL_SIZE : consumeThreadPoolSize;
        /**
         * 内部参数初始化
         */
        state = new AtomicBoolean(false);
        messageCount = new AtomicInteger(0);
        taskCallback = taskCallback == null ? new DefaultTaskCallback(consumer, queueName, messageCount) : taskCallback;
        receivethreadPoolExecutor = new ThreadPoolExecutor(receiveThreadPoolSize, receiveThreadPoolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        consumethreadPoolExecutor = new ThreadPoolExecutor(consumeThreadPoolSize, consumeThreadPoolSize,
                1L, TimeUnit.HOURS,
                new LinkedBlockingQueue<>());
    }


    /**
     * 消费
     */
    private void consume() {
        state.set(true);
        Runnable receiveTask = () -> {
            while (state.get()) {
                //若消息堆积数量大于消费线程池大小，则进行线程让步
                int count = messageCount.get();
                if (count >= consumeThreadPoolSize) {
                    Thread.yield();
                    continue;
                }
                try {
                    //预增加消息堆积数
                    if (!messageCount.compareAndSet(count, count + 1)) {
                        continue;
                    }
                    ReceiveResult receiveResult = consumer.receiveMsg(queueName);
                    if (receiveResult.getReturnCode() == ResponseCode.SUCCESS) {
                        //若接受消息成功，则递交给消费线程池处理
                        consumethreadPoolExecutor.execute(new ConsumeTask(receiveResult.getMessage(), messageListener, taskCallback));
                    } else {
                        //若接受消息失败，则回退消息堆积数
                        messageCount.decrementAndGet();
                    }
                } catch (Exception e) {
                }
            }
        };
        for (Integer i = 0; i < receiveThreadPoolSize; i++) {
            receivethreadPoolExecutor.execute(receiveTask);
        }
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public Integer getReceiveThreadPoolSize() {
        return receiveThreadPoolSize;
    }

    public void setReceiveThreadPoolSize(Integer receiveThreadPoolSize) {
        this.receiveThreadPoolSize = receiveThreadPoolSize;
    }

    public Integer getConsumeThreadPoolSize() {
        return consumeThreadPoolSize;
    }

    public void setConsumeThreadPoolSize(Integer consumeThreadPoolSize) {
        this.consumeThreadPoolSize = consumeThreadPoolSize;
    }
}
