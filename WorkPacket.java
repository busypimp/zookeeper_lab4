import java.io.Serializable;
import java.util.List;


public class WorkPacket implements Serializable{

	/*What kind of packet is this*/
	public static final int PART_REQUEST = 100;
	public static final int NEW_TASK = 101;
	public static final int IN_PROGRESS = 102;
	public static final int PASSWORD_FOUND = 103;
	public static final int PASSWORD_NOT_FOUND = 104;
	public static final int PART_REPLY = 105;
	public static final int ACK = 106;
	public static final int NULL = 107;
	/*Packet Fields*/
	public int job_id = 0;
	public int part_id = 0;
	public String passwordHash = null;
	public int total_partitions = 0;
	public int worker_id = 0;
	public List<String> partition = null;
	public int type = WorkPacket.NULL;
	public String solution = null;
}
