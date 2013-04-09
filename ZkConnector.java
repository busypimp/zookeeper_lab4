import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;


public class ZkConnector implements Watcher {
	private ZooKeeper zooKeeper;
	private CountDownLatch latch = new CountDownLatch(1);
	

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
            latch.countDown();
        }
		
	}
	
	public void connect(String hosts){

        try {
			zooKeeper = new ZooKeeper(hosts, 5000, this);
			latch.await();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
    }
	
	public void close(){
		try {
			zooKeeper.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ZooKeeper getZooKeeper(){
		if(zooKeeper == null || zooKeeper.getState().equals(States.CONNECTED)){
			System.err.println("ZOOKEEPER NOT CONNECTED!!");
			return null;
		}else{
			return this.zooKeeper;
		}
	}    
	
}
