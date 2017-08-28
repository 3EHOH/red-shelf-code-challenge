


public interface EnrollmentDataInterface {
	
	public Map<String, List<Enrollment>> getAllEnrollments ();
	public void addSourceFile (String s);
	public EnrollmentInputCounts getCounts();
	public List<String> getErrorMsgs();
	public InputStatistics getInputStatistics();
	
	public class EnrollmentInputCounts
	{
		public int enrollmentRecordsRead = 0;
		public int enrollmentRecordsAccepted = 0;
		public int enrollmentRecordsRejected = 0;
		public int getRecordsRead() {return enrollmentRecordsRead;}
		public int getRecordsAccepted() {return enrollmentRecordsAccepted;}
		public int getRecordsRejected() {return enrollmentRecordsRejected;}
	}

}
