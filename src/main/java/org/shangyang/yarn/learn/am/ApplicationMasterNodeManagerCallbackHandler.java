package org.shangyang.yarn.learn.am;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler;

/**
 * 
 * @author 商洋
 *
 * @createTime：Dec 11, 2016 12:24:15 PM
 * 
 */
public class ApplicationMasterNodeManagerCallbackHandler implements CallbackHandler{

    private static final Log LOGGER = LogFactory.getLog( ApplicationMasterNodeManagerCallbackHandler.class );
    

    //调用 startContainerAsync 之后返回的消息
    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
    	
    	LOGGER.info("container id:"+containerId+" started ");
    	
    }

    //下面两个方法只要我们不向NM大人发送请求就不会被调用，所以可以忽略。

    //调用getContainerStatusAsync之后返回的消息
    @Override
    public void onContainerStatusReceived( ContainerId containerId, ContainerStatus containerStatus ) {
    	
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("少年，NM大人回消息给你了你的计算单元 containerId=" + containerId + " containerStatus=" + containerStatus);
        }
    }

    // 调用stopContainerAsync之后返回的消息
    @Override
    public void onContainerStopped(ContainerId containerId) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("你成功干掉一个计算资源 containerId=" + containerId);
        }
    }

    //下面三个错误都是ApplicationMaster抛出，表示发送某个RPC失败
    @Override
    public void onStartContainerError(ContainerId containerId, Throwable throwable) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Start都发不了你是不是和NM大人失去联系了？ containerId=" + containerId, throwable);
        }
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("发个RPC都出错，Hadoop不给力啊 containerId=" + containerId, throwable);
        }
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable throwable) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("关个进程都挂掉，Hadoop不给力啊 containerId=" + containerId, throwable);
        }
    }

}
