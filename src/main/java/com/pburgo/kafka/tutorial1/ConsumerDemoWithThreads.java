package com.pburgo.kafka.tutorial1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    public static void main(String[] args) {

        CountDownLatch latch = new CountDownLatch(1);

        Runnable consumerRunnable = new ConsumerRunnable(latch);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.error("Caught shutdown hook!!");
            ((ConsumerRunnable)consumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                logger.info("Applicacion has exited!!");
            }

        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Applicacion closed!!");
        }
    }
}
