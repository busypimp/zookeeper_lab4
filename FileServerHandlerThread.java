import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FileServerHandlerThread extends Thread {
	private Socket sock = null;
	private String dictionary = null;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	private WorkPacket packetFromClient;
	private List<String> file_in;
	private Map intToListMap;

	public FileServerHandlerThread(Socket accept, String dictionaryFile) {
		// TODO Auto-generated constructor stub
		this.dictionary = dictionaryFile;
		this.sock = accept;
		this.file_in = readFile();
		this.intToListMap = new HashMap();
	}
	
	private List<String> readFile() {
		List<String> fileLinesList = new ArrayList<String>();
		try{
			BufferedReader input_stream = new BufferedReader(new FileReader(this.dictionary));
			String line ="";
			while ((line = input_stream.readLine()) != null) {
				fileLinesList.add(line);
			}
		}catch(FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileLinesList;
	}

	public void run(){
		try {
			this.in = new ObjectInputStream(sock.getInputStream());
			this.out = new ObjectOutputStream(sock.getOutputStream());
			initPartitions();
			
			while((this.packetFromClient = (WorkPacket) in.readObject()) != null){
				switch (packetFromClient.type){
				case WorkPacket.PART_REQUEST:
					processPartRequest();
					break;
				case WorkPacket.ACK:
					System.out.println("Ack received");
					break;
				default:
					System.err.println("ERROR: Unknown packet!!");
					System.exit(-1);
				}
			}
			out.close();
			in.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void processPartRequest() {
		System.out.println("Part request received!!");
		
		WorkPacket packetToClient = new WorkPacket();
		packetToClient.type = WorkPacket.PART_REPLY;
		packetToClient.worker_id = this.packetFromClient.worker_id;
		packetToClient.part_id = this.packetFromClient.part_id;
		int key = this.packetFromClient.part_id;
		if(this.intToListMap.containsKey(key))	{
			packetToClient.partition = (List<String>) this.intToListMap.get(key);
			sendPacket(packetToClient);
		}else{
			System.err.println("Unknown part id <"+ key + "> requested!");
		}
	}

	private void sendPacket(WorkPacket packetToClient) {
		try {
			out.writeObject(packetToClient);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void initPartitions() {
		// TODO Auto-generated method stub
		int fileSize = this.file_in.size();
		int firstIndex, secondIndex;
		List<String> partitionI;
		
		for(int i = 0; i < 4; i++){
			firstIndex = i*fileSize;
			secondIndex = ((i+1)*fileSize - 1);
			
			partitionI = this.file_in.subList(firstIndex, secondIndex);
			this.intToListMap.put(i+1, partitionI);
			
		}
//		List<String> partition_one = this.file_in.subList(0, fileSize - 1);
//		List<String> partition_two = this.file_in.subList(fileSize, (2*fileSize) - 1);
//		List<String> partition_three = this.file_in.subList((2*fileSize), (3*fileSize) - 1);
//		List<String> partition_four = this.file_in.subList((3*fileSize), (4*fileSize) - 1);
		
	}

}
