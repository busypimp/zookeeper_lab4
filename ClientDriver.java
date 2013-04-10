import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

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
	private static boolean redirect = false;
	private ZkConnector connector;
	private Watcher watcher;
	private static Socket sock = null;
	private static ObjectOutputStream out = null;
	private static ObjectInputStream in = null;
//	private static String userInput;

	public ClientDriver(String host) {
		connector = new ZkConnector();
		connector.connect(host);
		watcher = getNewWatcher();

	}

	private Watcher getNewWatcher() {
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
			initSocket();
			System.out.println("Streams online!");
			 
			if (!establishConnection()) {
				return;
			}
			runRequestConsole();
	 
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	private static void runRequestConsole() throws IOException, ClassNotFoundException {
		
		String userInput;
		
		System.out.println("=====================Request Console========================");
		System.out.println();
		System.out.println("Enter job request/status/query. Enter 'quit' to exit");
		System.out.println();
		System.out.print(">");
		
		/* Get user input */
		BufferedReader readIn = new BufferedReader(new InputStreamReader(System.in));
		while((userInput = readIn.readLine()) != null && !userInput.equalsIgnoreCase("quit")){
			Scanner userScan = new Scanner(userInput);
			String command = userScan.next();
			
			if(command.equalsIgnoreCase("request")){
				processRequestCommand(userScan);
			}else if(command.equalsIgnoreCase("status")){
				processStatusCommand();
			}else{
				System.err.println("Unknown Command Please try again!");
				System.out.print(">");
			}
			
			/*Once the packet is sent we wait for reply*/
			JobPacket packetFromServerNew = new JobPacket();
			packetFromServerNew = (JobPacket) in.readObject();
			
			if (packetFromServerNew.type == JobPacket.REQUEST_ACK) { 
				System.out.println("Primary JobTracker has received the job request and is processing it");
			}			
			else if (packetFromServerNew.type == JobPacket.IN_PROGRESS) { 
				System.out.println("The job request is still in progress ... ");
			}			
			else if (packetFromServerNew.type == JobPacket.PASSWORD_FOUND) { 
				System.out.println("The job request has completed and the password is " + packetFromServerNew.result);
			}			
			else if (packetFromServerNew.type == JobPacket.PASSWORD_NOT_FOUND) { 
				System.out.println("The job request has completed and the password was not found");
			} 
			else { 
				System.out.println ("UNRECOGNIZABLE packet with type" + packetFromServerNew.type);
			}
			
			System.out.print("> ");			
		}
		/*we have quit*/
		JobPacket packetToServer = new JobPacket();
		packetToServer.type = JobPacket.QUIT;
		sendToServer(packetToServer);
		System.out.println("Client Driver Shutting Down ... ");
		
		out.close();
		in.close();
		readIn.close();
		sock.close();
		
	}

	private static void processStatusCommand() {
		if(redirect){
			
			redirect = false;
			initSocket();
			
		}
		
		JobPacket packetToServer = new JobPacket();
		packetToServer.type = JobPacket.JOB_STATUS;
		
		sendToServer(packetToServer);
	}

	private static void processRequestCommand(Scanner scan) {
		if (redirect) {
			
			redirect = false;
			initSocket();
			
		}
		
		String passwordHashed = scan.next();
		
		JobPacket packetToServer = new JobPacket();
		packetToServer.type = JobPacket.JOB_REQUEST;
		packetToServer.passwordHash = passwordHashed;
		
		sendToServer(packetToServer);

	}

	private static void initSocket() {
		try {
			sock = new Socket(hostName, 8000);
			in = new ObjectInputStream(sock.getInputStream());
			out = new ObjectOutputStream(sock.getOutputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static boolean establishConnection() {
		try {
			JobPacket packetToServer = new JobPacket();
			packetToServer.type = JobPacket.ESTABLISH_CONNECTION;
			sendToServer(packetToServer);
			/* wait for the server to reply back to the request */
			JobPacket packet = (JobPacket) in.readObject();
			if(packet.type == JobPacket.CONNECTION_ACK){
				System.out.println("Connection established with primary job tracker. Ready for requests...");
				return true;
			}else{
				System.err.println("Trouble connecting to primary job tracker");
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
//		return false;
		return false;
	}

	private static void sendToServer(JobPacket packetToServer) {
		try {
			out.writeObject(packetToServer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private void getJobTrackerIP() {
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
