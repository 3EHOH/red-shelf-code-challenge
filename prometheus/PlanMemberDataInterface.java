


public interface PlanMemberDataInterface {
	
	public Map<String, PlanMember> getAllMembers ();
	public void addSourceFile (String s);
	public MemberInputCounts getCounts();
	public List<String> getErrorMsgs();
	public InputStatistics getInputStatistics();
	
	public class MemberInputCounts
	{
		public int memberRecordsRead = 0;
		public int memberRecordsAccepted = 0;
		public int memberRecordsRejected = 0;
		public int getRecordsRead() {return memberRecordsRead;}
		public int getRecordsAccepted() {return memberRecordsAccepted;}
		public int getRecordsRejected() {return memberRecordsRejected;}
	}

}
