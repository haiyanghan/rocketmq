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
package org.apache.rocketmq.client.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;

import java.util.List;

public class ConsumerReplyMessageHookImpl implements ConsumeMessageHook {

    private String consumerGroup;
    private MQClientInstance mqClientInstance;

    public ConsumerReplyMessageHookImpl(String consumerGroup, MQClientInstance mqClientInstance) {
        this.consumerGroup = consumerGroup;
        this.mqClientInstance = mqClientInstance;
    }

    @Override
    public String hookName() {
        return "ConsumerReplyMessageHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {

    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        List<MessageExt> msgList = context.getMsgList();
        for (MessageExt message : msgList) {
            if (MessageUtil.isReplyMsg(message)) {
                replyMessageConsumerResultToBroker(context.getStatus(), message);
            }
        }
    }

    private void replyMessageConsumerResultToBroker(String consumerResult, MessageExt message) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        ReplyMessageRequestHeader requestHeader = new ReplyMessageRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setConsumerResult(consumerResult);
        requestHeader.setConsumerTimeStamp(System.currentTimeMillis());
        requestHeader.setTopic(message.getTopic());
        requestHeader.setFlag(message.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
        requestHeader.setTransactionId(message.getTransactionId());

        String brokerAddress = this.mqClientInstance.findBrokerAddressInPublish(message.getBrokerName());
        if (StringUtils.isNoneBlank(brokerAddress)) {
            this.mqClientInstance.getMQClientAPIImpl().replyMessageConsumerResultToBroker(brokerAddress, requestHeader, message.getBody());
        }
    }
}
