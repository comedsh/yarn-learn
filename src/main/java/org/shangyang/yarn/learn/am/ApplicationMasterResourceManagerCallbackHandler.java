package org.shangyang.yarn.learn.am;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

/**
 * 
 * @author 商洋
 *
 * @createTime：Dec 11, 2016 10:35:20 AM
 * 
 */
public class ApplicationMasterResourceManagerCallbackHandler implements AMRMClientAsync.CallbackHandler {

	private static Log LOGGER = LogFactory.getLog(ApplicationMasterResourceManagerCallbackHandler.class);

	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager;
	
	private NMClientAsync nodeManager;
	
	private AtomicBoolean stopped = new AtomicBoolean(false);
	
	private boolean failure = false; // indicates there has errors, perhaps the partial fail or the full fail.
	
	private int containers = 0; // how many containers the Application Master get applied.
	
	private AtomicInteger processed = new AtomicInteger(0);
	
	public ApplicationMasterResourceManagerCallbackHandler( Configuration conf ){
		
		nodeManager = new NMClientAsyncImpl( new ApplicationMasterNodeManagerCallbackHandler() );
		
		nodeManager.init(conf);
		
		nodeManager.start();
		
	}
	
	public void setResourceManager(AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager) {

		this.resourceManager = resourceManager;
	}

	/**
	 * 
	 * 注意，不是当 Container 的相关任务完成，会回调此方法；而是逐步的进行回调...., 看下面 Application Master 的输出日志，
	 * 
	 * 16/12/11 13:56:46 INFO am.ApplicationMasterResourceManagerCallbackHandler: onContainersCompleted() called, totally get 1 replied ~~~~
	 * 
	 * 看到没，只返回了一个 Container 的状态信息，所以，这里需要统计反馈的信息，然后才能关闭
	 * 
	 * @param statues, 返回相关的状态信息。
	 * 
	 */
	@Override
	public void onContainersCompleted(List<ContainerStatus> statuses) {
		
		LOGGER.info("onContainersCompleted() called, totally get " + statuses.size() + " replied ~~~~");
		
        // 如果你要尝试重启某些任务可以在这里做
		
        for (ContainerStatus status : statuses) {
        	
        	int exitStatus = status.getExitStatus();
            
            // Container return code 非 0 表示失败
            if ( 0 != exitStatus ) {
            	
                LOGGER.warn( String.format( "容器挂了 ContainerID=%s Diagnostics=%s", status.getContainerId(), status.getDiagnostics() ) );
                
                failure = true;
                
            }
            
        }
        
        processed.addAndGet( statuses.size() );        
        
        LOGGER.info("onContainersCompleted() called, increased the processed number with " + statuses.size() + ", and the current processed number is "+ processed.get() );
        
        if( processed.get() == containers ){
        	
        	this.stop();
        }
        
	}
	
	/**
	 * 一次并不会申请所有的所需的 Containers，往往一次只能申请部分，或者更少；我单机测试的时候，大多数一次只能分配一个 Container... 
	 * 
	 * 16/12/11 14:11:04 INFO am.ApplicationMasterResourceManagerCallbackHandler: onContainersAllocated() get called, you have get totally 1 containers for computation 
	 * 
	 * 
	 * @param containers
	 */
	@Override
	public void onContainersAllocated( List<Container> containers ) {

		LOGGER.info("onContainersAllocated() get called, you have get totally " + containers.size() + " containers for computation ");

		for (Container c : containers) {
			
			LOGGER.info( "Container " + c.getId() + " takes the Memory Size: " + c.getResource().getMemorySize() + "; the cpu vitual cores: " + c.getResource().getVirtualCores() );
			
			ContainerLaunchContext launchContext = Records.newRecord( ContainerLaunchContext.class );

			StringBuilder cmd = new StringBuilder();
			
			// -cN: N 表示要 ping 多少次
			cmd.append("ping -c100 www.baidu.com").append(" ").append("1>")
			   .append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(Path.SEPARATOR)
			   .append(ApplicationConstants.STDOUT).append(" ").append("2>")
			   .append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(Path.SEPARATOR)
			   .append(ApplicationConstants.STDERR);

			launchContext.setCommands( Collections.singletonList( cmd.toString() ) );
			
			setupLaunchContextTokens( launchContext );
			
			nodeManager.startContainerAsync( c, launchContext );

		}

	}

    //当 yarn kill 的时候会调用这个刚发
	@Override
	public void onShutdownRequest() {
		
        stopped.set(true);
	}

	// 这个方法很美好，意思是指集群中增加了新的 NodeManager（扩容）但是现在 Hadoop 还不支持"在线"伸缩
	@Override
	public void onNodesUpdated(List<NodeReport> updatedNodes) {

	}

	@Override
	public float getProgress() {
		
		return 0;
	}

    // 本地的线程挂掉的时候会调用这个，如果你要提高可用性可以尝试在这里重启 AMRMClient
	@Override
	public void onError(Throwable e) {
		
		LOGGER.error(e.getMessage(), e);
		
		stopped.set( true );
	}
	
	protected void setupLaunchContextTokens(ContainerLaunchContext ctx) {
		
		try {
			
			Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
			
			DataOutputBuffer dob = new DataOutputBuffer();
			
			credentials.writeTokenStorageToStream(dob);
			
			Iterator<org.apache.hadoop.security.token.Token<?>> iterator = credentials.getAllTokens().iterator();
			
			while (iterator.hasNext()) {
				
				org.apache.hadoop.security.token.Token<?> token = iterator.next();
				
				LOGGER.debug("Token " + token);
				
				if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
					
					iterator.remove();
				}
			}
			
			ByteBuffer allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
			
			ctx.setTokens(allTokens);
			
		} catch (IOException e) {
			
			LOGGER.error("Error setting tokens for launch context", e );
		}
		
	}
	
	/**
	 * 关闭 Application Master 与 Resource Manager 之间的连接
	 * 关闭 Application Master 与 Node Manager 之间的连接 
	 */
	void stop(){
		
        nodeManager.stop();
        
        try {
        	
        	// 取消 Application Master 与 Resource Manager 之间的注册关系；两者之间不再关联
			resourceManager.unregisterApplicationMaster( failure == false ? FinalApplicationStatus.SUCCEEDED : FinalApplicationStatus.FAILED, 
														 failure == false ? "成功了" : "失败了", 
														 null );
			
		} catch (YarnException | IOException e) {

			LOGGER.error(e, e.getCause() );
			
			e.printStackTrace();
			
			throw new RuntimeException(e);
			
		}
        
        resourceManager.stop();		
	}

	public void setContainers( int containers ) {

		this.containers = containers;
	}
	
}
