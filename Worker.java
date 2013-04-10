import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.net.*;
import java.io.*;
import java.util.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

// Worker Thread that will send request packet to the FileServer to get the requested partition

public class Worker {
	
	static String FilePath = "/FileServer";
	ZkConnector zkConn;
	Watcher watcher;
	
	static String WorkerPath = "/WorkerRoot";
	static Queue q;
	static Queue workers;
	
	static String PacketPath = "/workPackets";
	static Date current_date;
	static String string_date;
	
	// Create the I/O streams
	Socket fs_socket = null;
	ObjectOutputStream out = null;
	ObjectInputStream in = null;
	
	static String myName;

	static int jobId;
	static int partId;
	static int partitions;
	static String password;
	
	static String serialized_ret;
	static String serialized_ret2;

	static WorkPacket retrieved;
	
	public static void main (String[] args) {
		
		if (args.length != 1) {
			System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
			return;
		}
		
		// Consume the packet on the ZooKeeper
		String stringJT = null;
		String zk_host = (String) args[0];
		System.out.println("The host is: " + zk_host);
		
		current_date = new Date();
		string_date = current_date.toString();
		
		try {
			myName = java.net.InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {}
		
		myName = myName.concat(".utoronto.ca");
		System.out.println("My address is: " + myName + " and the date is: " + string_date);
		
		// Register the workers with the ZooKeeper for the JobTracker to find it
		workers = new Queue(zk_host, WorkerPath);
		
		try {
			workers.produce_worker(myName, string_date);
		} catch (KeeperException e) {
			System.out.println("KeeperException is " + e.toString());
		} catch (InterruptedException e) {
			System.out.println("Interrupted Exception!!");
		}	

		Scanner readinput = new Scanner(System.in);
		System.out.println("Waiting for input");
		String myinput = readinput.nextLine();
		
		q = new Queue(zk_host, PacketPath);		
		System.out.println("About to go to the consumer block");		
		
		try {
			stringJT = q.consume();
		} catch (KeeperException e) {
			System.out.println("KeeperException is " + e.toString());
		} catch (InterruptedException e) {
			System.out.println("Interrupted Exception!!");
		}	
		
		// Retrieve information from the Node 
		retrieved = new WorkPacket();
		retrieved = deserializePacket(stringJT);
	
		jobId = retrieved.job_id;
		partId = retrieved.part_id;
		partitions = retrieved.total_partitions;
		password = (String) retrieved.passwordHash;
								
		System.out.println("The job_id is: " + jobId);
		System.out.println("The part_id is: " + partId);
		System.out.println("The total number of partitions is: " + partitions);
		System.out.println("The passwordHash is: " + password);
		System.out.println("The type is: " + retrieved.type);
		System.out.println("The solution is: " + retrieved.solution);
		
		System.out.println("Updating the Node status to: IN_PROGRESS");
		// Change the Node type to IN_PROGRESS and update it on the ZooKeeper
		retrieved.type = WorkPacket.IN_PROGRESS;
		serialized_ret = serializePacket(retrieved);
		
		try {
			q.update_status(serialized_ret, jobId, partId);
		} catch (KeeperException e) {
			System.out.println("KeeperException is " + e.toString());
		} catch (InterruptedException e) {
			System.out.println("Interrupted Exception!!");
		}		
		
		Worker t = new Worker(args[0]);		

		System.out.println("Sleeping...");
		try {
			Thread.sleep(1000);
		} catch (Exception e) {}
		
		t.extractip();
		
		System.out.println("Sleeping...");
		
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {}
		}
	}
	
	public Worker (String hosts) {
		zkConn = new ZkConnector();
		
		try {
			zkConn.connect(hosts);
		} catch (Exception e) {
			System.out.println("Zookeeper connect " + e.getMessage());
		}
		
		watcher = new Watcher() {
							@Override
							public void process (WatchedEvent event) {
								handleEvent(event);
							} };
	}
	
	private void extractip() {
//		Stat stat = zkc.exists(FilePath, watcher);
		
//		if (stat == null) {
////			System.out.println("Node does not exist");
//		}
//		else {
			ZooKeeper zkeep = zkConn.getZooKeeper();
			byte[] ServAddr_array = null;
			
			try {
				
					try {
						ServAddr_array = zkeep.getData(FilePath, false, null);
					} catch (Exception e) {}
				
				// Initialize to FileServer and form the stream
				String ServAddress = new String(ServAddr_array);
				String addr = ServAddress.concat(".utoronto.ca");
				System.out.println("The address of the FileServer is " + addr);
				
				System.out.println("Creating the required sockets");
				fs_socket = new Socket(addr, 8333);
				out = new ObjectOutputStream(fs_socket.getOutputStream());
				in = new ObjectInputStream(fs_socket.getInputStream());
				
				WorkPacket packetFromServer;
				
				System.out.println("DEBUG: Worker is here!");
				
				// Send packet to the FileServer
				WorkPacket packetToServer = new WorkPacket();
				packetToServer.type = WorkPacket.PART_REQUEST;
				packetToServer.part_id = partId;
				packetToServer.total_partitions = partitions;
				packetToServer.worker_id = 1;
				out.writeObject(packetToServer);

				List<String> dictionary = null;
				
				while ((packetFromServer = (WorkPacket) in.readObject()) != null) {

					System.out.println("Reading from Server");
					System.out.println("The value read from server is: " + packetFromServer.part_id);
					
					dictionary = new ArrayList<String>(packetFromServer.partition.size());
					copyPartition(packetFromServer.partition, dictionary);				
					
					WorkPacket packetToServer2 = new WorkPacket();
					packetToServer2.type = WorkPacket.ACK;
					out.writeObject(packetToServer2);
					break;
				}
				
				out.close();
				in.close();
				// System.out.println("About to show contents: ");
				// showContents(dictionary);
				System.out.println("Worker exit here!");

				// Given a Password Hash, compute the Hash of every entry in the Dictionary and compare to find if there is a match
				String current = null;
				String hash_current = null;
				
				System.out.println("The hash to be found is: " + password);
				
				int j = 0;
				
				/*current = dictionary.get(j);
				hash_current = getHash(current);
				System.out.println("The computed hash is: " + hash_current);
				*/
				// Update the Status of the packet 
				retrieved.type = WorkPacket.PASSWORD_NOT_FOUND;				
				
				for (j = 0; j < dictionary.size(); j++) {
					current = dictionary.get(j);
					hash_current = getHash(current);
					// System.out.println("Computed hash is: " + hash_current);
						
						if (hash_current.equals(password)) {
							System.out.println("Hash found!!");
							retrieved.solution = current;
							retrieved.type = WorkPacket.PASSWORD_FOUND;
							break;
						}
						
						if (hash_current.equalsIgnoreCase(password)) {
							System.out.println("Hash found!!: IGNORE CASE");
							retrieved.solution = current;
							retrieved.type = WorkPacket.PASSWORD_FOUND;
							break;
						}

						if ((hash_current.compareTo(password)) == 0) {
							System.out.println("Hash found!!: COMPARE TO");
							retrieved.solution = current;
							retrieved.type = WorkPacket.PASSWORD_FOUND;
							break;
						}
				}
				
				System.out.println("Updating status after search");
				serialized_ret2 = serializePacket(retrieved);	
				
				try {
					q.update_status(serialized_ret2, jobId, partId);
				} catch (KeeperException e) {
					System.out.println("KeeperException is " + e.toString());
				} catch (InterruptedException e) {
					System.out.println("Interrupted Exception!!");
				}		

				System.out.println("Finished searching for Hash");
			} catch (IOException e) {	
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();		
			}
//		}
	}
	
	private void handleEvent (WatchedEvent event) {
		String path = event.getPath();
		EventType type = event.getType();
		
		if (path.equalsIgnoreCase(FilePath)) {
			if (type == EventType.NodeDeleted) {
				System.out.println("Node Deleted!!");
				extractip();
			}
			if (type == EventType.NodeCreated) {
				System.out.println("Node Created: Extract IP");
				
				try {
					Thread.sleep(1000);
				} catch (Exception e) {}
				
				extractip();
			}
		}		
	}
	
	public void showContents (List<String> partition) {
		Iterator it = partition.iterator();
		int j = 0;
		
		while ((it.hasNext()) && (j < 50)) {
			System.out.println("The value contained is " + it.next());
			j++;
		}
	}
	
	public void copyPartition (List<String> src, List<String> dest) {
		int i = 0;
		
		for (i = 0; i < src.size(); i++) {
			dest.add(src.get(i));
		}
	}
	
	public static String getHash (String word) {
		String hash = null;
		
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
			hash = hashint.toString(16);
			while (hash.length() < 32) hash = "0" + hash;
		} catch (NoSuchAlgorithmException nsae) {}
		
		return hash;
	}
	
	static public String serializePacket(WorkPacket workPacket) { 
		String serializedPacket = workPacket.job_id + ":" + workPacket.part_id + ":" + workPacket.total_partitions + ":" + 
			workPacket.passwordHash + ":" + workPacket.type + ":" + workPacket.solution;
		System.out.println(serializedPacket);
		return serializedPacket;
	}
	
	static public WorkPacket deserializePacket (String serializedPacket) { 
			
		StringTokenizer st = new StringTokenizer(serializedPacket, ":");
		WorkPacket workPacket = new WorkPacket();
		workPacket.job_id = Integer.parseInt(st.nextToken());
		workPacket.part_id = Integer.parseInt(st.nextToken());
		workPacket.total_partitions = Integer.parseInt(st.nextToken());
		workPacket.passwordHash = st.nextToken();
		workPacket.type = getType(st.nextToken());
		workPacket.solution = st.nextToken();
			
		return workPacket;
	}	
	
	static public int getType (String type) { 
		int returnType = WorkPacket.NULL;
			
			if (type.equalsIgnoreCase("IN_PROGESS")) { 
				returnType = WorkPacket.IN_PROGRESS;			
			} else if (type.equalsIgnoreCase("PASSWORD_FOUND")) { 
				returnType = WorkPacket.PASSWORD_FOUND;
			} else if (type.equalsIgnoreCase("PASSWORD_NOT_FOUND")) { 
				returnType = WorkPacket.PASSWORD_NOT_FOUND;
			}
			
		return returnType;
	}
}


