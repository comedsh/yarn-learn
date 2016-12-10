package im.lsn.learnyarn.test;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Before;
import org.junit.Test;

import im.lsn.learnyarn.DemoApplicationClient;

/**
 * Created by xingsen on 2016/10/7.
 */
public class DemoTest {
	
	private Configuration conf;

	@Before
	public void before() throws Exception {
		
		// 首先得启动本地的 Yarn 服务
		conf = new YarnConfiguration();
		
		conf.set(YarnConfiguration.RM_ADDRESS, "localhost:8032");
		
		conf.set(YarnConfiguration.RM_HOSTNAME, "localhost");
		
		conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, "localhost:8030");
		
		conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "localhost:8031");
		
		conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:8088");
		
		conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);

		// 启动 Yarn Clusters
		@SuppressWarnings("resource")
		MiniYARNCluster yrCluster = new MiniYARNCluster("test", 1, 1, 1);
		
		yrCluster.init( conf );
		
		yrCluster.start();
	}

	@Test
	public void testClient() throws IOException, YarnException, InterruptedException {
		
		DemoApplicationClient client = new DemoApplicationClient(conf);
		
		String jarPath = new File(".").getCanonicalPath() + "/target/learn-yarn-1.0.0-SNAPSHOT.jar";
		
		client.setAppMasterJar( jarPath );
		
		ApplicationId applicationId = client.submit();
		
		boolean outAccepted = false;
		
		boolean outTrackingUrl = false;
		
		// 下面的代码的用途是监控 application master 的执行状态。
		ApplicationReport report = client.getApplicationReport( applicationId );
		
		while (report.getYarnApplicationState() != YarnApplicationState.FINISHED) {
			
			report = client.getApplicationReport( applicationId );
			
			if (!outAccepted && report.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
				
				System.out.println("Application is accepted use Queue=" + report.getQueue() + " applicationId="+ report.getApplicationId());
				
				outAccepted = true;
			}
			
			if (!outTrackingUrl && report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
				
				String trackingUrl = report.getTrackingUrl();
				
				System.out.println("Master Tracking URL = " + trackingUrl);
				
				outTrackingUrl = true;
			}
			
			System.out.println(String.format("%f %s", report.getProgress(), report.getYarnApplicationState()));
			
			try {
				
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
		System.out.println( report.getFinalApplicationStatus() );
		
		while (true) {
			
			Thread.sleep(1000);
		}
	}
}
