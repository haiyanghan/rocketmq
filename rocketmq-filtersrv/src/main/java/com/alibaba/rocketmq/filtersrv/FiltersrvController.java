/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.filtersrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.filtersrv.processor.DefaultRequestProcessor;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;


/**
 * Name Server服务控制
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-5
 */
public class FiltersrvController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);
    // Filter Server配置
    private final FiltersrvConfig filtersrvConfig;
    // 通信层配置
    private final NettyServerConfig nettyServerConfig;
    // 服务端通信层对象
    private RemotingServer remotingServer;
    // 服务端网络请求处理线程池
    private ExecutorService remotingExecutor;

    // 访问Broker的api封装
    private final FilterServerOuterAPI filterServerOuterAPI = new FilterServerOuterAPI();

    // 定时线程
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NamesrvControllerScheduledThread");
            }
        });


    public FiltersrvController(FiltersrvConfig filtersrvConfig, NettyServerConfig nettyServerConfig) {
        this.filtersrvConfig = filtersrvConfig;
        this.nettyServerConfig = nettyServerConfig;
    }


    public boolean initialize() {
        // 打印服务器配置参数
        MixAll.printObjectProperties(log, this.filtersrvConfig);

        // 初始化通信层
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);

        // 初始化线程池
        this.remotingExecutor =
                Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);


                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "RemotingExecutorThread_" + threadIndex.incrementAndGet());
                    }
                });

        this.registerProcessor();

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
            }
        }, 10, 10, TimeUnit.SECONDS);

        return true;
    }


    public String localAddr() {
        return String.format("%s:%d", this.filtersrvConfig.getFilterSrvIP(),
            this.remotingServer.localListenPort());
    }


    public void registerFilterServerToBroker() {
        try {
            this.filterServerOuterAPI.registerFilterServerToBroker(
                this.filtersrvConfig.getConnectWhichBroker(), this.localAddr());
        }
        catch (Exception e) {
        }
    }


    private void registerProcessor() {
        this.remotingServer
            .registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
    }


    public void start() throws Exception {
        this.remotingServer.start();
    }


    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
    }


    public RemotingServer getRemotingServer() {
        return remotingServer;
    }


    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }


    public ExecutorService getRemotingExecutor() {
        return remotingExecutor;
    }


    public void setRemotingExecutor(ExecutorService remotingExecutor) {
        this.remotingExecutor = remotingExecutor;
    }


    public FiltersrvConfig getFiltersrvConfig() {
        return filtersrvConfig;
    }


    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }


    public FilterServerOuterAPI getFilterServerOuterAPI() {
        return filterServerOuterAPI;
    }
}
