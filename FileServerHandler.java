import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;



import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


public class FileServerHandler {
	private String localHost;
	private ZkConnector connector;
	private Watcher zkWatcher;
	private ServerSocket sock = null;
	private String connectorString;
	private List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
	private String dictionaryFile;
	private static String path = "/FileServer";
	
	public FileServerHandler(String connString, String host, String file){
		this.connectorString = connString;
		this.connector = new ZkConnector();
//		connector.connect();
		zkWatcher = getCustomWatcher();
		localHost = host;
		dictionaryFile = file;
		
	}
	
	
	
	private Watcher getCustomWatcher() {
		// TODO Auto-generated method stub
		return new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				handleEvent(event);
			}

		};
	}

	private void handleEvent(WatchedEvent event) {
		String eventPath = event.getPath();
		EventType type = event.getType();
		
		if(eventPath.equalsIgnoreCase(path)){
			if(type == EventType.NodeDeleted){
				checkPath();
			}else if(type == EventType.NodeCreated){
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				checkPath();
			}
		}
		
	}
	
	public void checkPath() {
		try {
			Stat stat = connector.getZooKeeper().exists(localHost, zkWatcher);
			if(stat == null){
				System.out.println("zNode crated <"+ path +">");
				String ret = connector.getZooKeeper().create(path, localHost.getBytes(), acl, CreateMode.EPHEMERAL);
				sock = new ServerSocket(8333);
				while (true) {
					new FileServerHandlerThread(sock.accept(), dictionaryFile).start();
				}		
			}else{
				ZooKeeper zookeeper = connector.getZooKeeper();
				byte[] ip = zookeeper.getData(path, false, null);
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
