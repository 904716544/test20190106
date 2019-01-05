/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package customDrpc;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.Config;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.supervisor.DefaultUncaughtExceptionHandler;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.generated.DistributedRPC;
import org.apache.storm.generated.DistributedRPCInvocations;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.server.THsHaServer;
import org.apache.storm.thrift.transport.TNonblockingServerSocket;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import backtype.storm.Config;
//import backtype.storm.daemon.Shutdownable;
//import backtype.storm.generated.DRPCExecutionException;
//import backtype.storm.generated.DRPCRequest;
//import backtype.storm.generated.DistributedRPC;
//import backtype.storm.generated.DistributedRPCInvocations;

//import com.alibaba.jstorm.callback.AsyncLoopRunnable;
//import com.alibaba.jstorm.callback.AsyncLoopThread;
//import com.alibaba.jstorm.cluster.StormConfig;
//import com.alibaba.jstorm.utils.JStormUtils;
//import com.alibaba.jstorm.utils.TimeUtils;

/**
 * Drpc
 * 
 * @author yannian
 * 
 */
public class CustomDrpc implements DistributedRPC.Iface, DistributedRPCInvocations.Iface, Shutdownable {

    private static final Logger LOG = LoggerFactory.getLogger(CustomDrpc.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Begin to start Drpc server");

        Thread.setDefaultUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());

        final CustomDrpc service = new CustomDrpc();

        service.init();
    }

    private Map conf;

    private THsHaServer handlerServer;

    private THsHaServer invokeServer;

//    private AsyncLoopThread clearThread;

    private AtomicBoolean shutdown = new AtomicBoolean(false);

    private THsHaServer initHandlerServer(Map conf, final CustomDrpc service) throws Exception {
        int port = 13772;
        int workerThreadNum = 64;
        int queueSize = 64;
        
        LOG.info("Begin to init Handler Server " + port);

        TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
        THsHaServer.Args targs = new THsHaServer.Args(socket);
        targs.maxWorkerThreads(64);
        targs.protocolFactory(new TBinaryProtocol.Factory());
        targs.processor(new DistributedRPC.Processor<DistributedRPC.Iface>(service));

        ThreadPoolExecutor executor = new ThreadPoolExecutor(workerThreadNum, workerThreadNum, 60, TimeUnit.SECONDS, new ArrayBlockingQueue(queueSize));
        targs.executorService(executor);

        THsHaServer handlerServer = new THsHaServer(targs);
        LOG.info("Successfully init Handler Server " + port);

        return handlerServer;
    }

    private THsHaServer initInvokeServer(Map conf, final CustomDrpc service) throws Exception {
        int port = 13773;
        
        LOG.info("Begin to init Invoke Server " + port);

        TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
        THsHaServer.Args targsInvoke = new THsHaServer.Args(socket);
        targsInvoke.maxWorkerThreads(64);
        targsInvoke.protocolFactory(new TBinaryProtocol.Factory());
        targsInvoke.processor(new DistributedRPCInvocations.Processor<DistributedRPCInvocations.Iface>(service));

        THsHaServer invokeServer = new THsHaServer(targsInvoke);

        LOG.info("Successfully init Invoke Server " + port);
        return invokeServer;
    }

    private void initThrift() throws Exception {

        handlerServer = initHandlerServer(conf, this);

        invokeServer = initInvokeServer(conf, this);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                CustomDrpc.this.shutdown();
                handlerServer.stop();
                invokeServer.stop();
            }

        });

        LOG.info("Starting Distributed RPC servers...");
        new Thread(new Runnable() {

            @Override
            public void run() {
                invokeServer.serve();
            }
        }).start();
        handlerServer.serve();
    }

    private void initClearThread() {
//        clearThread = new AsyncLoopThread(new ClearThread(this));
        LOG.info("Successfully start clear thread");
    }
    private void createPid(Map conf) throws Exception {
//        String pidDir = StormConfig.drpcPids(conf);
//
//        JStormServerUtils.createPid(pidDir);
    }

    public void init() throws Exception {
//        conf = StormConfig.read_storm_config();
        LOG.info("Configuration is \n" + conf);

//        createPid(conf);

        initClearThread();

        initThrift();
    }

    public CustomDrpc() {
    }

    @Override
    public void shutdown() {
        if (shutdown.getAndSet(true) == true) {
            LOG.info("Notify to quit drpc");
            return;
        }

        LOG.info("Begin to shutdown drpc");
//        AsyncLoopRunnable.getShutdown().set(true);

//        clearThread.interrupt();

//        try {
//            clearThread.join();
//        } catch (InterruptedException e) {
//        }
        LOG.info("Successfully cleanup clear thread");

        invokeServer.stop();
        LOG.info("Successfully stop invokeServer");

        handlerServer.stop();
        LOG.info("Successfully stop handlerServer");

    }

    private AtomicInteger ctr = new AtomicInteger(0);
    private ConcurrentHashMap<String, Semaphore> idtoSem = new ConcurrentHashMap<String, Semaphore>();
    private ConcurrentHashMap<String, Object> idtoResult = new ConcurrentHashMap<String, Object>();
    private ConcurrentHashMap<String, Integer> idtoStart = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, String> idtoFunction = new ConcurrentHashMap<String, String>();
    private ConcurrentHashMap<String, DRPCRequest> idtoRequest = new ConcurrentHashMap<String, DRPCRequest>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> requestQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>>();

    public void cleanup(String id) {
        LOG.info("clean id " + id + " @ " + (System.currentTimeMillis()));

        idtoSem.remove(id);
        idtoResult.remove(id);
        idtoStart.remove(id);
        idtoFunction.remove(id);
        idtoRequest.remove(id);
    }

    @Override
    public String execute(String function, String args) throws DRPCExecutionException, TException {
        LOG.info("Received DRPC request for " + function + " " + args + " at " + (System.currentTimeMillis()));
        int idinc = this.ctr.incrementAndGet();
        int maxvalue = 1000000000;
        int newid = idinc % maxvalue;
        if (idinc != newid) {
            this.ctr.compareAndSet(idinc, newid);
        }

        String strid = String.valueOf(newid);
        Semaphore sem = new Semaphore(0);

        DRPCRequest req = new DRPCRequest(args, strid);
      this.idtoStart.put(strid, (int)Time.currentTimeMillis() / 1000);
        this.idtoSem.put(strid, sem);
        this.idtoFunction.put(strid, function);
        this.idtoRequest.put(strid, req);
        ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(function);
        queue.add(req);
        LOG.info("Waiting for DRPC request for " + function + " " + args + " at " + (System.currentTimeMillis()));
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            LOG.error("acquire fail ", e);
        }
        LOG.info("Acquired for DRPC request for " + function + " " + args + " at " + (System.currentTimeMillis()));

        Object result = this.idtoResult.get(strid);
        if (!this.idtoResult.containsKey(strid)){
            result = new DRPCExecutionException("Request timed out");   // this request is timeout, set exception
        }
        LOG.info("Returning for DRPC request for " + function + " " + args + " at " + (System.currentTimeMillis()));

        this.cleanup(strid);

        if (result instanceof DRPCExecutionException) {
            throw (DRPCExecutionException) result;
        }
        return String.valueOf(result);
    }

    @Override
    public void result(String id, String result) throws TException {
        Semaphore sem = this.idtoSem.get(id);
        LOG.info("Received result " + result + " for id " + id + " at " + (System.currentTimeMillis()));
        if (sem != null) {
            this.idtoResult.put(id, result);
            sem.release();
        }

    }

    @Override
    public DRPCRequest fetchRequest(String functionName) throws TException {

        ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(functionName);
        DRPCRequest req = queue.poll();
        if (req != null) {
            LOG.info("Fetched request for " + functionName + " at " + (System.currentTimeMillis()));
            return req;
        } else {
            return new DRPCRequest("", "");
        }

    }

    @Override
    public void failRequest(String id) throws TException {
        Semaphore sem = this.idtoSem.get(id);
        LOG.info("failRequest result  for id " + id + " at " + (System.currentTimeMillis()));
        if (sem != null) {
            this.idtoResult.put(id, new DRPCExecutionException("Request failed"));
            sem.release();
        }
    }

    protected ConcurrentLinkedQueue<DRPCRequest> acquireQueue(String function) {
        ConcurrentLinkedQueue<DRPCRequest> reqQueue = requestQueues.get(function);
        if (reqQueue == null) {
            reqQueue = new ConcurrentLinkedQueue<DRPCRequest>();
            requestQueues.put(function, reqQueue);
        }
        return reqQueue;
    }

    public ConcurrentHashMap<String, Semaphore> getIdtoSem() {
        return idtoSem;
    }

    public ConcurrentHashMap<String, Object> getIdtoResult() {
        return idtoResult;
    }

    public ConcurrentHashMap<String, Integer> getIdtoStart() {
        return idtoStart;
    }

    public ConcurrentHashMap<String, String> getIdtoFunction() {
        return idtoFunction;
    }

    public ConcurrentHashMap<String, DRPCRequest> getIdtoRequest() {
        return idtoRequest;
    }

    public AtomicBoolean isShutdown() {
        return shutdown;
    }

    public Map getConf() {
        return conf;
    }

}
