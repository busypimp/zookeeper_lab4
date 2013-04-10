import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Barrier extends SyncPrimitive {

	private String name;
	private int size;

	Barrier(String connectString, String root, int size) {
		super(connectString);
		try {

			this.root = root;
			this.size = size;
			this.name = InetAddress.getLocalHost().getCanonicalHostName();

			if (this.zookeeper == null) {
				return;
			} else {
				createBarrierNode();
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void createBarrierNode() throws KeeperException, InterruptedException {
		Stat stat = zookeeper.exists(this.root, false);
		if(stat == null){
			zookeeper.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}else{
			System.out.println("The root <" + this.root + "> already exists");
		}

	}
	
	boolean enter() throws KeeperException, InterruptedException {
		zookeeper.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		while (true) {
			synchronized (mutex) {
				List<String> list = zookeeper.getChildren(root, true);
				if (list.size() < size) {
					mutex.wait();
				} else {
					return true;
				}
			}
		}
	}

	boolean leave() throws KeeperException, InterruptedException {
		zookeeper.delete(root + "/" + name, 0);
		while (true) {
			synchronized (mutex) {
				List<String> list = zookeeper.getChildren(root, true);
				if (list.size() > 0) {
					mutex.wait();
				} else {
					return true;
				}
			}
		}
	}

}
