package com.tal.cmq.test;

import com.qcloud.cmq.client.consumer.Consumer;
import com.tal.cmq.consumer.ConsumerBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ConsumerTest {
    Consumer consumer;
    private ConsumerBean consumerBean;
    private String queueName;

    @Before
    public void before() throws Exception {
        consumer = new Consumer();
        consumer.setNameServerAddress(Properties.nameServerAddress);
        consumer.setSecretId(Properties.secretId);
        consumer.setSecretKey(Properties.secretKey);
        queueName = Properties.queueName;
        consumerBean = new ConsumerBean(consumer, queueName, (message) -> {
            //TimeUnit.SECONDS.sleep(10);
            System.out.println(message);
        }, 1, 16);

        consumer.start();
        consumerBean.start();
    }

    @Test
    public void test() throws Exception {
        TimeUnit.SECONDS.sleep(10);
    }

    @After
    public void after() {
        consumerBean.shutdown();
        consumer.shutdown();
    }
}
