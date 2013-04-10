import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;


public class JobTrackerHandlerThread extends Thread{
	private Socket socket;
	private String connString;
	private int jobID;
	private ZooKeeper zkeep;
	private String passwordHash = null;
	private Queue q;
	//SyncPrimitive.Queue w;
	private static final String rootWorkerPackets = "/workPackets";
	private static final String rootWorkersCount = "/WorkerRoot";
	
	public JobTrackerHandlerThread (Socket sock, String conn, int ID, ZkConnector zk) {
		super("JobTrackerHandlerThread");
		this.socket = sock;
		this.connString = conn;
		this.jobID = ID;
		this.zkeep = zk.getZooKeeper();
	}
	public void run() {
		boolean gotByePacket = false;
		
		try {
			/* stream to read from client */
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());
			JobPacket packetFromClient;
			
			/* stream to write back to client */
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());
//			q = new SyncPrimitive.Queue(zkAddress, rootWorkerPackets);
			//w = new SyncPrimitive.Queue(zkAddress, rootWorkersCount);
			
			while ((packetFromClient = (JobPacket) fromClient.readObject()) != null) {
				/* create a packet to send reply back to client */
				JobPacket packetToClient = new JobPacket();
				
				if (packetFromClient.type == JobPacket.ESTABLISH_CONNECTION) {									
					System.out.println("Obtained Establish Connection packet");
					packetToClient.type = JobPacket.CONNECTION_ACK;								
					toClient.writeObject(packetToClient);
					System.out.println("Finished writing to outputstream 1");
					continue;
				}
				
				
				if (packetFromClient.type == JobPacket.JOB_REQUEST) {									
					System.out.println("Obtained Job Request from Client");
					this.passwordHash  = packetFromClient.passwordHash;
					createWorkPackets();
					packetToClient.type = JobPacket.REQUEST_ACK;								
					toClient.writeObject(packetToClient);
					System.out.println("Finished writing to outputstream 2");
					continue;
				}
				
				if (packetFromClient.type == JobPacket.JOB_STATUS) {									
					System.out.println("Obtained Job Status Query from Client");
					try {
						toClient.writeObject(processStatusQuery());
						System.out.println("Finished writing to outputstream 3");
					} catch (KeeperException e) {						
						e.printStackTrace();
					} catch (InterruptedException e) {						
						e.printStackTrace();
					}
//					packetToClient.type = JobPacket.Type.REQUEST_ACK;								
//					toClient.writeObject(packetToClient);
					continue;
				} 
								
				/* Sending a JOB_NULL || JOB_BYE means quit */
				if (packetFromClient.type == JobPacket.NULL || packetFromClient.type == JobPacket.QUIT) {
					gotByePacket = true;
					System.out.println ("Received QUIT Packet from Client");
					packetToClient.type = JobPacket.QUIT;
					toClient.writeObject(packetToClient);
					System.out.println("Finished writing to outputstream 4");
					break;
				}
					
				System.err.println("ERROR: Unknown JOB_* packet!!");
				System.exit(-1);
			}
			
			System.out.println("Exiting while loop");
			
			/* cleanup when client exits */
			fromClient.close();
			toClient.close();
			socket.close();

		} catch (IOException e) {
			if(!gotByePacket)
				e.printStackTrace();
		} catch (ClassNotFoundException e) {
			if(!gotByePacket)
				e.printStackTrace();
		}
	}

	private JobPacket processStatusQuery() throws KeeperException, InterruptedException { 
		

		System.out.println("Entered method : processStatusQuery() ");
		JobPacket packetToClient = new JobPacket();
		try {
			int statusVal = 0;
			List<String> workPackets = q.consumeBasedOnJobID(jobID);
			List<WorkPacket> deserializedWorkPackets = new ArrayList<WorkPacket>();
			for (String s : workPackets) { 
				System.out.println("Serialized packet --> " + s);
				deserializedWorkPackets.add(deserializePacket(s));
			}
			for (WorkPacket w : deserializedWorkPackets) { 
							
				System.out.println(w.type);					
				if (w.type == WorkPacket.PASSWORD_FOUND) { 					
					packetToClient.type = JobPacket.PASSWORD_FOUND;
					packetToClient.result = w.solution;
					System.out.println("The solution is: " + w.solution);
					System.out.println("The result is: " + packetToClient.result);
					statusVal = 2;
					break;
				} else if (w.type == WorkPacket.IN_PROGRESS) {					
					packetToClient.type = JobPacket.IN_PROGRESS;
					statusVal = 3;
					break;
				} else if (w.type == WorkPacket.PASSWORD_NOT_FOUND) {
					statusVal = -1;
				} else {
					System.out.println("No matching type found");
				}
			}
			System.out.println("statusVal = " + statusVal);
			if (statusVal == -1) { 
				packetToClient.type = JobPacket.PASSWORD_NOT_FOUND;
			}			
			
		} catch (KeeperException e) {			
			e.printStackTrace();
		} catch (InterruptedException e) {			
			e.printStackTrace();
		}
		return packetToClient;
		
	}	

	private void createWorkPackets() { 
		try {
			List<String> list = zkeep.getChildren(rootWorkersCount, false);
			int total_partitions = list.size();
			System.out.println("The number of workers is:" + total_partitions);

			if (total_partitions == 0 ) { 
				createWorkPacket(0, 1);
			} else { 			
				for ( int i = 0 ; i < total_partitions; i++) { 
					createWorkPacket(i, total_partitions);
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	private void createWorkPacket(int partition_id , int total) {
		
		System.out.println("Entering createWorkPacket method");
		WorkPacket workPacket = new WorkPacket();
		workPacket.job_id = jobID;
		workPacket.part_id = partition_id + 1;
		workPacket.total_partitions = total;
		workPacket.passwordHash = passwordHash; 
		workPacket.type = WorkPacket.NEW_TASK;
		workPacket.solution = "~";
		
		String serial_Packet = serializePacket(workPacket);		
		
		try {
			q.produce(serial_Packet ,jobID , partition_id+1);
		} catch (KeeperException e1) {			
			e1.printStackTrace();
		} catch (InterruptedException e1) {			
			e1.printStackTrace();
		}		
	}	
	
	private String serializePacket(WorkPacket workPacket) { 
		System.out.println("Entering serializePacket method");		
		String serializedPacket = workPacket.job_id + ":" + workPacket.part_id + ":" + workPacket.total_partitions + ":" + 
			workPacket.passwordHash + ":" + workPacket.type + ":" + workPacket.solution;
		System.out.println(serializedPacket);
		return serializedPacket;
	}
	
	private WorkPacket deserializePacket (String serializedPacket) { 
		System.out.println("Entering deserializePacket method");
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
	
	private int getType (String type) { 
		System.out.println("Entering  getType method");
		int returnType = WorkPacket.NULL;
		if (type.equalsIgnoreCase("IN_PROGESS") || type.equalsIgnoreCase("NEW_TASK") ) { 
			returnType = WorkPacket.IN_PROGRESS;			
		} else if (type.equalsIgnoreCase("PASSWORD_FOUND")) { 
			returnType = WorkPacket.PASSWORD_FOUND;
		} else if (type.equalsIgnoreCase("PASSWORD_NOT_FOUND")) { 
			returnType = WorkPacket.PASSWORD_NOT_FOUND;
		}
		return returnType;
	}
}
