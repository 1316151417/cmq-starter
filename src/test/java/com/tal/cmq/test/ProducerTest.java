package com.tal.cmq.test;

import com.qcloud.cmq.client.producer.Producer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProducerTest {

    private Producer producer;
    private String queueName;

    @Before
    public void before() throws Exception {
        producer = new Producer();
        producer.setNameServerAddress(Properties.nameServerAddress);
        producer.setSecretId(Properties.secretId);
        producer.setSecretKey(Properties.secretKey);
        queueName = Properties.queueName;

        producer.start();
    }

    @Test
    public void test() throws Exception {
        for (int i = 0; i < 1000; i++) {
            producer.send(queueName, "hello " + i);
        }
    }

    @After
    public void after() {
        producer.shutdown();
    }
}
