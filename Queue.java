import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Queue extends SyncPrimitive {

	Queue(String connectionString, String name) {
		super(connectionString);
		this.root = name;

		if (zookeeper == null)
			return;
		createQueueNode();
	}

	private void createQueueNode() {
		try {
			Stat stat = zookeeper.exists(root, false);
			if (stat == null) {
				zookeeper.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public boolean produce(String packet, int Job_ID, int Part_ID)
			throws KeeperException, InterruptedException {

		byte[] byteData = null;
		if (packet != null) {
			byteData = packet.getBytes();
		}
		zookeeper.create(root + "/element" + Job_ID + "_" + Part_ID, byteData,
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

		System.out.println("created node :"
				+ zookeeper.getChildren(root, false).get(0));
		return true;
	}

	// New consume method, each worker will read the first element of the queue
	public String consume() throws KeeperException, InterruptedException {
		String content = null;
		String serial = null;

		synchronized (mutex) {
			List<String> list = zookeeper.getChildren(root, false); 

			if (list.size() == 0) {
				System.out.println("Empty List: No children exist!!");
				mutex.wait();
			} else {
				// Get the first element of this list to process
				content = list.get(0);

				// Get the data contained within this child node
				System.out.println("Reading from: " + root + "/" + content);
				byte[] b = zookeeper.getData(root + "/" + content, false, null);
				serial = new String(b);
				System.out.println("The content of the packet read is " + root
						+ "/" + content + " " + serial);
				return serial;
			}
		}

		return serial;
	}

	public List<String> consumeBasedOnJobID(int job_ID) throws KeeperException,
			InterruptedException {

		List<String> workPackets = new ArrayList<String>();

		synchronized (mutex) {
			List<String> list = zookeeper.getChildren(root, false);
			for (String s : list) {
				Integer jobID = new Integer(s.substring(7, 8));
				if (jobID == job_ID) {
					byte[] b = zookeeper.getData(root + "/" + s, false, null);
					String serializedPacket = new String(b);
					if (serializedPacket.indexOf("PASSWORD_FOUND") != -1
							|| serializedPacket.indexOf("PASSWORD_NOT_FOUND") != -1) {
						zookeeper.delete(root + "/" + s, -1);
					}
					workPackets.add(serializedPacket);
				}
			}
		}
		return workPackets;
	}

	public boolean produce_worker(String hostname, String date)
			throws KeeperException, InterruptedException {

		byte[] data_array = null;

		if (hostname != null) {
			data_array = hostname.getBytes();
		}

		zookeeper.create(root + "/worker" + hostname + "_" + date, data_array,
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		return true;
	}

	public int num_workers() throws KeeperException, InterruptedException {
		int counter = 0;

		synchronized (mutex) {
			List<String> list = zookeeper.getChildren("/WorkerRoot", false);
			counter = list.size();
		}

		System.out.println("The number of workers is: " + counter);
		return counter;
	}

	public boolean update_status(String packet, int job_id, int part_id)
			throws KeeperException, InterruptedException {
		byte[] newData = null;
		newData = packet.getBytes();

		synchronized (mutex) {
			System.out.println("Updating Node: " + root + "/element" + job_id
					+ "_" + part_id + "0000000000");
			zookeeper.setData(root + "/element" + job_id + "_" + part_id
					+ "0000000000", newData, -1);
		}

		return true;
	}
}
