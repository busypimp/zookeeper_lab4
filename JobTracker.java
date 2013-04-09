import java.io.IOException;
import java.net.UnknownHostException;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import org.apache.zookeeper.Watcher;

public class JobTracker {
	private static String localHost;
	private static String connectorString;
	////private static int connectorPort;
	private static String dictionaryFile;
	private static ServerSocket sock = null;
//	static boolean listening = true;
//	private static int jobNum = 0;

	private static String USAGE_MESSAGE = "Usage: java -classpath"+
	" lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker "+
			"zkServer:clientPort"; 
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length != 1) {
				System.out.println(USAGE_MESSAGE);
				return;
			}else{
				connectorString = args[0];

			}
			localHost = java.net.InetAddress.getLocalHost().getHostName();
			JobTrackerHandler handler = new JobTrackerHandler(connectorString, localHost);
//			Thread.sleep(5000);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
 
		
	}
	
//	public static synchronized int getJobID () {
//		return jobNum++;
//	}

}
