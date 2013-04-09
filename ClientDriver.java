import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ClientDriver {

	private static String path = "/JobTracker";
	private static String USAGE_MESSAGE = "";

	private static String hostName;
	private static String zooKeeperServerHost;
	private boolean redirect = false;
	private ZkConnector connector;
	private Watcher watcher;
	private static Socket sock = null;
	private static ObjectOutputStream out = null;
	private static ObjectInputStream in = null;
	private static String userInput;

	public ClientDriver(String host) {
		// TODO Auto-generated constructor stub
		connector = new ZkConnector();
		connector.connect(host);
		watcher = getNewWatcher();

	}

	private Watcher getNewWatcher() {
		// TODO Auto-generated method stub
		return new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				handleEvent(event);

			}

		};
	}

	private void handleEvent(WatchedEvent event) {
		String path = event.getPath();
		EventType type = event.getType();
		try {

			if (path.equalsIgnoreCase(path)) {
				if (type == EventType.NodeDeleted) {
					System.out.println("Primary Job Tracker down");
					Thread.sleep(5000);
					getJobTrackerIP();
				} else if (type == EventType.NodeCreated) {
					System.out.println("Primary Job Tracker connected to ZooKeeper");
					Thread.sleep(5000);
					getJobTrackerIP();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 1) {
			System.out.println(USAGE_MESSAGE);
			return;
		} else {
			zooKeeperServerHost = args[0];
		}

		try {
			ClientDriver cd = new ClientDriver(zooKeeperServerHost);
			Thread.sleep(5000);
			cd.getJobTrackerIP();
			 sock = new Socket(hostName, 8000);
			 in = new ObjectInputStream(sock.getInputStream());
			 out = new ObjectOutputStream(sock.getOutputStream());
			 System.out.println("Streams online!");
			 /*Get user input*/
			 BufferedReader readIn = new BufferedReader(new InputStreamReader(System.in));
			 
			 JobPacket packetToServer = new JobPacket();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void getJobTrackerIP() {
		// TODO Auto-generated method stub
		ZooKeeper zookeeper = connector.getZooKeeper();
		try {
			Stat stat = zookeeper.exists(path, watcher);
			if (stat == null) {
				System.err.println("No job tracker exixts for <" + path + ">");
				return;
			}
			byte[] IPaddr_array = zookeeper.getData(path, false, null);
			if (hostName != null) {
				redirect = true;
			}
			hostName = new String(IPaddr_array);
			hostName = hostName.concat(".toronto.edu");
			System.out.println("The IP address of the primary JobTracker is: <"
					+ hostName + ">");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
