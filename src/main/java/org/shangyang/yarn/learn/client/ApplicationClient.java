package org.shangyang.yarn.learn.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.shangyang.yarn.learn.am.ApplicationMaster;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ApplicationClient {

	private static Log LOGGER = LogFactory.getLog( ApplicationClient.class );

	private final Configuration conf;

	private final String appMasterMainClass;

	private final YarnClient yarnClient;

	private String appMasterJar;

	/**
	 * 初始化 Yarn Client；
	 * 
	 * @param conf
	 */
	public ApplicationClient( Configuration conf ) {

		this.conf = conf;

		this.appMasterJar = ClassUtil.findContainingJar( ApplicationMaster.class );

		this.appMasterMainClass = ApplicationMaster.class.getName();

		yarnClient = YarnClient.createYarnClient();

		yarnClient.init( conf );

		yarnClient.start();
	}

	public void setAppMasterJar(String appMasterJar) {

		this.appMasterJar = appMasterJar;
	}

	/**
	 * 
	 * 做了如下几件事情，
	 * 
	 * 1. 初始化 Client 并试图创建 Application Master 
	 * 2. 设置 Application Master 所需资源, CPU 和 内存 
	 *    通过查询当前集群中单个 node 的最大可用资源，来合理生成 Application Master 所需资源
	 * 3. 设置 Application Master 执行的 commands
	 * 4. 设置 Application Master 执行所需的 jar
	 *    jar 必须放置到 HDFS 上，这样 NodeManager 才可以获得该 jar
	 * 5. 设置 Application Master 执行所需的环境变量
	 * 6. 设置 Credential
	 * 7. submit the Application Master
	 * 
	 * 
	 * @return
	 * @throws IOException
	 * @throws YarnException
	 */
	public ApplicationId submit() throws IOException, YarnException {

		FileSystem fs = FileSystem.get( conf );

		YarnClientApplication app = yarnClient.createApplication();

		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		
		// 这里我的理解上有歧义，到底是返回当前集群可用最大的资源的总和？还是返回的是集群中单个 node 的最大可用资源？我倾向于后一种认知，既是当前单个 node 最大的可用资源。
		Resource clusterMax = appResponse.getMaximumResourceCapability(); // 集群最大资源?

		// 
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

		// 定义 Container 所需的资源 -> am: Application Master
		Resource amResource = Records.newRecord( Resource.class );
		
		// 单机，资源有限，合理配置
		amResource.setMemorySize( Math.min( clusterMax.getMemorySize(), 256 ) );

		amResource.setVirtualCores( Math.min(clusterMax.getVirtualCores(), 1 ) );

		appContext.setResource( amResource );

		// 初始化 Container 启动环境 Context
		ContainerLaunchContext containerLaunchContext = Records.newRecord( ContainerLaunchContext.class );
		
		// .. define execution commands, add it into the Container Context
		StringBuilder cmd = new StringBuilder();

		cmd.append( "\"" + ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java\"").append(" ").append( appMasterMainClass ).append(" ");
		
		if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
			
			cmd.append(" ").append("debug").append(" ");
		}

		/*
		 * 1> 指标准信息输出路径（也就是默认的输出方式）
		 * 2> 指错误信息输出路径
		 * 2>&1 指将标准信息输出路径指定为错误信息输出路径（也就是都输出在一起）
		 */
		cmd.append("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDOUT).append(" ")
		   .append("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDERR );
		
		containerLaunchContext.setCommands( Collections.singletonList( cmd.toString() ) );
		
		LOGGER.info( cmd.toString() );
		
		// .. add the execution jars into the Container Context
		Map<String, LocalResource> localResourceMap = new HashMap<String, LocalResource>();
		
		File appMasterJarFile = new File( appMasterJar );
		
		/** toLocalResource 方法，将 jar 文件放置在 HDFS 上， 并返回相关路径 **/
		localResourceMap.put( appMasterJarFile.getName(), toLocalResource( fs, appResponse.getApplicationId().toString(), appMasterJarFile ) );
		
		containerLaunchContext.setLocalResources( localResourceMap );
		
		// .. add the execution environments into the Container Context
		Map<String, String> envMap = new HashMap<String, String>();
		
		envMap.put( "JAVA_HOME", "/Library/Java/JavaVirtualMachines/jdk1.7.0_79.jdk/Contents/Home" );
		
		envMap.put( "CLASSPATH", hadoopClassPath() );
		
		envMap.put( "LANG", "en_US.UTF-8" );
		
		containerLaunchContext.setEnvironment( envMap );		

		// .. add the security token into the Container Context
		if (UserGroupInformation.isSecurityEnabled()) {
			
			String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
			
			if (tokenRenewer == null || tokenRenewer.length() == 0) {
				
				throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
			}
			
			Credentials credentials = new Credentials();
			
			org.apache.hadoop.security.token.Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
			
			if (LOGGER.isInfoEnabled()) {
				if (tokens != null) {
					for (org.apache.hadoop.security.token.Token<?> token : tokens) {
						LOGGER.info("Got dt for " + fs.getUri() + "; " + token);
					}
				}
			}
			
			DataOutputBuffer dob = new DataOutputBuffer();
			
			credentials.writeTokenStorageToStream(dob);
			
			containerLaunchContext.setTokens(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));
		}
		
		// Add
		appContext.setAMContainerSpec( containerLaunchContext );

		// Launch the application
		return yarnClient.submitApplication( appContext );
	}

	public ApplicationReport getApplicationReport(ApplicationId appId) throws IOException, YarnException {
		
		return yarnClient.getApplicationReport(appId);
	}

	protected String hadoopClassPath() {
		
		StringBuilder classPathEnv = new StringBuilder().append(File.pathSeparatorChar).append("./*");
		
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH) ) {
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(c.trim());
		}
		
		if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(System.getProperty("java.class.path"));
		}
		
		return classPathEnv.toString();
	}

	protected Path copyToHdfs(FileSystem fs, String appId, String srcFilePath) throws IOException {
		
		Path src = new Path(srcFilePath);
		
		String suffix = ".staging" + File.separator + appId + File.separator + src.getName();
		
		Path dst = new Path(fs.getHomeDirectory(), suffix);
		
		if (!fs.exists(dst.getParent())) {
			
			FileSystem.mkdirs(fs, dst.getParent(), FsPermission.createImmutable((short) Integer.parseInt("755", 8)));
		}
		
		fs.copyFromLocalFile(src, dst);
		
		return dst;
	}
	
	/**
	 * 
	 * 将 jarFile 放置到 HDFS 上，然后将该文件封装成 LocalResource 并返回给 Container 使用
	 * 
	 * @param fs
	 * @param appId
	 * @param jarFile
	 * @return
	 * @throws IOException
	 */
	private LocalResource toLocalResource(FileSystem fs, String appId, File jarFile) throws IOException {
		
		Path hdfsFile = copyToHdfs(fs, appId, jarFile.getPath());
		
		FileStatus stat = fs.getFileStatus(hdfsFile);
		
		return LocalResource.newInstance( ConverterUtils.getYarnUrlFromURI(hdfsFile.toUri()), LocalResourceType.FILE, LocalResourceVisibility.PRIVATE, stat.getLen(), stat.getModificationTime());
	}

	public static void main(String args[]) throws IOException, YarnException, InterruptedException {
		
		Configuration conf = new YarnConfiguration();
		
		ApplicationClient client = new ApplicationClient(conf);
		
		ApplicationId applicationId = client.submit();
		
		boolean outAccepted = false;
		
		boolean outTrackingUrl = false;
		
		ApplicationReport report = client.getApplicationReport( applicationId );
		
		while ( report.getYarnApplicationState() != YarnApplicationState.FINISHED ) {
			
			report = client.getApplicationReport(applicationId);
			
			if ( !outAccepted && report.getYarnApplicationState() == YarnApplicationState.ACCEPTED ) {
				
				LOGGER.info("Application is accepted use Queue=" + report.getQueue() + " applicationId=" + report.getApplicationId() );
				
				outAccepted = true;
				
			}
			
			if ( !outTrackingUrl && report.getYarnApplicationState() == YarnApplicationState.RUNNING ) {
				
				String trackingUrl = report.getTrackingUrl();
				
				LOGGER.info("Master Tracking URL = " + trackingUrl);
				
				outTrackingUrl = true;
				
			}
			
			LOGGER.info( String.format("%f %s", report.getProgress(), report.getYarnApplicationState() ) );
			
			Thread.sleep(1000);
			
		}
		
		LOGGER.info( report.getFinalApplicationStatus() );
	}
	
}
