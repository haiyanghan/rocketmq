/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class ProducerAndConsumer {

    /**
     * The number of produced messages.
     */
    public static final int MESSAGE_COUNT = 1000;
    public static final String CONSUMER_GROUP = "myConsumer";
    public static final String PRODUCER_GROUP = "myProduct";

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "test1";
    public static final String TAG = "TagA";

    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        /*
         * Specify name server addresses.
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         *  producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        /*
         * Launch the instance.
         */
        producer.start();

        try {



            /*
             * Call send message to deliver message to one of brokers.
             */
            for (int i = 0; i < 1; i++) {
                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
                Message msg = new Message(TOPIC /* Topic */,
                        TAG /* Tag */,
                        ("Hello RocketMQ," + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );
//                Message request = producer.request(msg, 5000);
                SendCallback sendCallback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println("call ...");
                    }

                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                        System.out.println("onException ...");
                    }
                };
                producer.request(msg, new RequestCallback() {
                    @Override
                    public void onSuccess(Message message) {
                        System.out.println("on success...");
                    }

                    @Override
                    public void onException(Throwable e) {

                    }
                }, 10000);
//                System.out.println(message);
            }
            /*
             * There are different ways to send message, if you don't care about the send result,you can use this way
             * {@code
             * producer.sendOneway(msg);
             * }
             */

            /*
             * if you want to get the send result in a synchronize way, you can use this send method
             * {@code
             * SendResult sendResult = producer.send(msg);
             * System.out.printf("%s%n", sendResult);
             * }
             */

            /*
             * if you want to get the send result in a asynchronize way, you can use this send method
             * {@code
             *
             *  producer.send(msg, new SendCallback() {
             *  @Override
             *  public void onSuccess(SendResult sendResult) {
             *      // do something
             *  }
             *
             *  @Override
             *  public void onException(Throwable e) {
             *      // do something
             *  }
             *});
             *
             *}
             */

            /*
             * Instantiate with specified consumer group name.
             */
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

            /*
             * Specify name server addresses.
             * <p/>
             *
             * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
             * <pre>
             * {@code
             * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
             * }
             * </pre>
             */
            // Uncomment the following line while debugging, namesrvAddr should be set to your local address
            consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

            /*
             * Specify where to start in case the specific consumer group is a brand-new one.
             */
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            if (args.length == 0) {
                args = new String[] {"TagA"};
            }
            /*
             * Subscribe one more topic to consume.
             */
            consumer.subscribe(TOPIC, MessageSelector.byTag(args[0]));

            /*
             *  Register callback to execute on arrival of messages fetched from brokers.
             */
            consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            /*
             *  Launch the consumer instance.
             */
            consumer.start();

            System.out.printf("Consumer Started.%n");

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(1000);
        }
    }
}
