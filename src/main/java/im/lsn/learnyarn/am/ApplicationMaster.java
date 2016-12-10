package im.lsn.learnyarn.am;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ApplicationMaster {
	
	public static void main(String[] args) throws IOException, YarnException, InterruptedException {
		
		Configuration conf = new YarnConfiguration();

		// 便于使用minicluster调试
		boolean isDebug = false;
		
		if (args.length >= 1 && args[0].equalsIgnoreCase("debug")) {
			isDebug = true;
		}
		
		if (isDebug) {

			conf.set(YarnConfiguration.RM_ADDRESS, "localhost:8032");
			
			conf.set(YarnConfiguration.RM_HOSTNAME, "localhost");
			
			conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, "localhost:8030");
			
			conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "localhost:8031");
			
			conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:8088");
			
			conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
			
		}
		
		AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient = AMRMClientAsync.createAMRMClientAsync( 1000, new AmRmCallbackHandler() );
		
		amRMClient.init( conf );
		
		amRMClient.start();
		
		String hostname = NetUtils.getHostname();
		
		// The ApplicationMaster needs to register itself with the ResourceManager to start hearbeating.
		// The timeout expiry interval at the RM is defined by a config setting accessible via YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS with the default being defined by YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS
		// 三个参数的意义，1) appHostName, AM's host name, 2) appHostPort, AM's host port, -1, random, 3) appTrakcingUrl，告诉 RM，我可以被 trakcing 的 URL 地址，比如，AM 启动了一个 netty web 服务，可以通过 URL 得知执行的状态信息。 
		amRMClient.registerApplicationMaster( hostname, -1, null );
		
		// 打印 10 次退出，避免 Application Master 一直在后台执行
		for( int i=0; i< 10; i++ ) {
			
			System.out.println("(stdout)Hello World");
			
			System.err.println("(stderr)Hello World");
			
			Thread.sleep(1000);
			
		}
		
		// 当 Application Master 执行完成以后，需要和 RM 之间解除 heart beating，并告诉它，已经成功完成了任务，否则，Application Master 并不会退出
		amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "application master successed", null);
		
	}
}
