import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


public class SyncPrimitive implements Watcher{
	
    static ZooKeeper zookeeper;
    static Integer mutex;
    String root;

	SyncPrimitive(String connectString) {
		try {
			if(zookeeper == null)
				return;
			zookeeper = new ZooKeeper(connectString, 3000, this);
			mutex = new Integer(-1);
		} catch (IOException e) {
			System.out.println(e.toString());
			return;
		}
	}

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            System.out.println("Process: " + event.getType());
            mutex.notify();
        }
    }
}
