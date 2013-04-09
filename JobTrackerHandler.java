import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


public class JobTrackerHandler {
  private String connString = null;
	private String localHost = null;
	private String path = "/Jobtracker";
	private ZkConnector zkConnector;
	private Watcher zkWatcher;
	private ServerSocket sock = null;
	private List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
	
	public JobTrackerHandler (String conn, String lhost) {
		this.connString = conn;
		this.localHost = lhost;
		this.zkConnector = new ZkConnector();
		this.zkWatcher= getCustomWatcher() ;
	}
	
	public static void main(String[] args) {
		try {
			sock = new ServerSocket(8000);        
	    	while (listening) {
	    		int Job_ID = getJobID();
	        	new JobTrackerHandlerThread(sock.accept(), args[0], Job_ID, zkc).start();
	        }
	    	sock.close();			
	    } catch (IOException e) {
	        System.err.println("ERROR: Could not listen on port!");
	        System.exit(-1);
	    } 
	}
	private Watcher getCustomWatcher() {
		return new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				handleEvent(event);
			}
		};
	}

	private void handleEvent(WatchedEvent event) {
		String path = event.getPath();
		EventType type = event.getType();
		
		if (path.equalsIgnoreCase(path)) {
			if (type == EventType.NodeDeleted) {
				System.out.println("Primary Job Tracker down");
				checkPath();
			}
			
			if (type == EventType.NodeCreated) {
				System.out.println("Primary Job Tracker connected");
				
				try {
					Thread.sleep(5000);
				} catch (Exception e) {}
				checkPath();
			}
		}
	}
	
	public void checkPath() {
		try {
			Stat stat = zkConnector.getZooKeeper().exists(path, zkWatcher);
			if (stat == null) {
				System.out.println("Creating znode: " + path);
				String ret = zkConnector.getZooKeeper().create(path, localHost.getBytes(), acl, CreateMode.EPHEMERAL);
			} else {
				ZooKeeper zk = zkConnector.getZooKeeper();
				byte[] IPaddr_array = zk.getData(path, false, null);
			}
		} catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
}
