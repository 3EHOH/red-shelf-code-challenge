


public interface ClaimDataInterface {
	
	public List<ClaimServLine> getAllServiceLines ();
	public void addSourceFile (String s);
	public ClaimInputCounts getCounts();
	public List<String> getErrorMsgs();
	public InputStatistics getInputStatistics();
	
	public class ClaimInputCounts
	{
		public int claimRecordsRead = 0;
		public int claimRecordsAccepted = 0;
		public int claimRecordsRejected = 0;
		public int getRecordsRead() {return claimRecordsRead;}
		public int getRecordsAccepted() {return claimRecordsAccepted;}
		public int getRecordsRejected() {return claimRecordsRejected;}
	}

}
