package im.lsn.learnyarn.am;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
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
		
		AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient = AMRMClientAsync.createAMRMClientAsync( 1000, new ApplicationMasterCallback() );
		
		amRMClient.init( conf );
		
		amRMClient.start();
		
		String hostname = NetUtils.getHostname();
		
		// The ApplicationMaster needs to register itself with the ResourceManager to start hearbeating.
		//  The timeout expiry interval at the RM is defined by a config setting accessible via YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS with the default being defined by YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS
		@SuppressWarnings("unused")
		RegisterApplicationMasterResponse registration = amRMClient.registerApplicationMaster(hostname, -1, "");
		
		while (true) {
			
			System.out.println("(stdout)Hello World");
			
			System.err.println("(stderr)Hello World");
			
			Thread.sleep(1000);
			
		}
	}
}
