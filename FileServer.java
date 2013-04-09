import java.net.ServerSocket;
import java.net.UnknownHostException;

import org.apache.zookeeper.Watcher;


public class FileServer {
	private static String localHost;
	private static String connectorString;
//	private static int connectorPort;
	private static String dictionaryFile;

	
	private static String USAGE_MESSAGE = "Usage: java -classpath"+
	" lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer "+
			"zkServer:clientPort <Dictionary File>"; 
	public FileServer(String connectorString2) {
		// TODO Auto-generated constructor stub
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			
			if (args.length != 2) {
				System.out.println(USAGE_MESSAGE);
				return;
			}else{
				connectorString = args[0];
				dictionaryFile = args[1];
			}
			localHost = java.net.InetAddress.getLocalHost().getHostName();
			FileServerHandler handler = new FileServerHandler(connectorString, localHost, dictionaryFile);
			Thread.sleep(5000);
			handler.checkPath();
			while(true){
				Thread.sleep(5000);
			}			
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
