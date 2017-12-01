


public interface ClaimRxDataInterface {
	
	public List<ClaimRx> getAllRxs ();
	public void addSourceFile (String s);
	public ClaimRxInputCounts getCounts();
	public List<String> getErrorMsgs();
	public InputStatistics getInputStatistics();
	
	public class ClaimRxInputCounts
	{
		public int claimRecordsRead = 0;
		public int claimRecordsAccepted = 0;
		public int claimRecordsRejected = 0;
		public int getRecordsRead() {return claimRecordsRead;}
		public int getRecordsAccepted() {return claimRecordsAccepted;}
		public int getRecordsRejected() {return claimRecordsRejected;}
	}

}
