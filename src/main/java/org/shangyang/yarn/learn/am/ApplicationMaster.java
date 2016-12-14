package org.shangyang.yarn.learn.am;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ApplicationMaster {
	
	private static Log LOGGER = LogFactory.getLog(ApplicationMaster.class);
	
	public static void main(String[] args) throws IOException, YarnException, InterruptedException {
		
		Configuration conf = new YarnConfiguration();

		// 便于使用 minicluster 调试
		boolean isDebug = false;
		
		if (args.length >= 1 && args[0].equalsIgnoreCase("debug")) {
			isDebug = true;
		}
		
		if ( isDebug ) {

			conf.set(YarnConfiguration.RM_ADDRESS, "localhost:8032");
			
			conf.set(YarnConfiguration.RM_HOSTNAME, "localhost");
			
			conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, "localhost:8030");
			
			conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "localhost:8031");
			
			conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:8088");
			
			conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
			
			conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
			
		}
		
		/**
		 * 备注：如果不需要 Application Master 再通过 Resource Manager 申请额外的 Container 再进行其它的事务，那么
		 * 
		 * 不需要
		 * 1) 不需要定义额外的 ApplicationMasterResourceManagerCallbackHandler。
		 * 2) 不需要通过 resourceManager 通过 ContainerRequest 再去申请额外的资源。
		 * 
		 * 只需要
		 * 1) 执行 Application Master 自己需要执行的代码即可。
		 * 2) 注册 Application Master 到 Resource Manager，告知我的运行状态
		 * 
		 */
		ApplicationMasterResourceManagerCallbackHandler applicationMasterResourceManagerCallbackHandler = new ApplicationMasterResourceManagerCallbackHandler( conf );
		
		// 当 RM 回调 AM 的时候，会触发 ApplicationClientCallbackHandler 方法, 第一个参数是心跳间隔
		AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager = AMRMClientAsync.createAMRMClientAsync( 1000, applicationMasterResourceManagerCallbackHandler );
		
		resourceManager.init( conf );
		
		resourceManager.start();
		
		// 将 resource manager 注入，这样方便 ApplicationMasterResourceManager 根据 node manager 的情况来控制两者的生命周期。
		applicationMasterResourceManagerCallbackHandler.setResourceManager( resourceManager );
		
		applicationMasterResourceManagerCallbackHandler.setContainers( 5 );
		
		/** The ApplicationMaster needs to register itself with the ResourceManager to start heart beating.
		    The timeout expiry interval at the RM is defined by a config setting accessible via YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS with the default being defined by YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS
		    三个参数的意义，
		    1) appHostName, AM's host name, 
		    2) appHostPort, AM's host port, -1, random, 
		    3) appTrakcingUrl，告诉 RM，我可以被 trakcing 的 URL 地址，比如，AM 启动了一个 netty web 服务，可以通过 URL 得知执行的状态信息。
		**/ 
		RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster( NetUtils.getHostname(), -1, null );

		Resource clusterMax = response.getMaximumResourceCapability();
		
		CountDownLatch latch = new CountDownLatch( 5 ); // 让 5 个线程同时执行
		
		for( int i=0; i< 5; i++ ) {
			
			LOGGER.info("Application Master start to request "+ i +" Container~");
			
			ContainerRequestRunnable rr = new ContainerRequestRunnable( clusterMax, resourceManager, latch );
			
			rr.setIndex(i);
			
			Thread t = new Thread( rr );
			
			t.start();
			
		}
		
	}
	
}

/**
 * 
 * Container Request Runnable Object.
 * 
 * @author 商洋
 *
 * @createTime：Dec 14, 2016 10:15:21 AM
 */
class ContainerRequestRunnable implements Runnable{
	
	private static Log LOGGER = LogFactory.getLog(ContainerRequestRunnable.class);
	
	Resource clusterMax;
	
	AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager;		
	
	CountDownLatch latch;
	
	int i;
	
	public ContainerRequestRunnable( Resource clusterMax, AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager, CountDownLatch latch ){
		
		this.clusterMax = clusterMax;
		
		this.resourceManager = resourceManager;
		
		this.latch = latch;
		
	}
	
	public void setIndex(int index){
		
		i = index;
	}

	@Override
	public void run() {
		
		this.latch.countDown();
		
		LOGGER.info("Starts to allocated the <"+ i +"> containers, negotiates with Resource Manager ~~~~");
		
		LOGGER.info("Available cluster memory size: " + clusterMax.getMemorySize() + " and cluster vitual cores : " + clusterMax.getVirtualCores() );
		
		LOGGER.info("Allocated the memory size: " + Math.min( clusterMax.getMemorySize(), 128 ) + " and the vitual cores : " + Math.min(clusterMax.getVirtualCores(), 1 ) );
		
		// 单机跑，资源有限，慎用
		Resource capability = Resource.newInstance( Math.min( clusterMax.getMemorySize(), 128 ), Math.min(clusterMax.getVirtualCores(), 1 ) );
		
		AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest( capability, 
															 							null, // 让 RM 帮我们选择 Node
															 							null, // 让 RM 帮我们选择 RACKS
															 							Priority.newInstance(0) );
		
		// AM 向 RM 正式提交请求，当请求成功，会回调 ApplicationClientCallbackHandler
		resourceManager.addContainerRequest( containerRequest );				
		
	}
	
}

