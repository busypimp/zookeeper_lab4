import java.io.Serializable;



public class JobPacket implements Serializable {

	/*Types of packet*/
	public static final int JOB_REQUEST = 100;
	public static final int JOB_STATUS = 101;
	public static final int QUIT = 102;
	public static final int ESTABLISH_CONNECTION = 103;
	public static final int CONNECTION_ACK = 104;
	public static final int REQUEST_ACK = 105;
	public static final int NULL = 106;
	public static final int PASSWORD_FOUND = 107;
	public static final int PASSWORD_NOT_FOUND = 108;
	public static final int IN_PROGRESS = 109;
	
	/*Packet fields*/
	public String passwordHash = null;
	public int type = JobPacket.NULL;
	public String result = null;
	
}