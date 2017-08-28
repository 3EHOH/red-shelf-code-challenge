


public interface ProviderDataInterface {
	
	public Map<String, Provider> getAllProviders ();
	public void addSourceFile (String s);
	public ProviderInputCounts getCounts();
	public List<String> getErrorMsgs();
	public InputStatistics getInputStatistics();
	
	public class ProviderInputCounts
	{
		int providerRecordsRead = 0;
		int providerRecordsAccepted = 0;
		int providerRecordsRejected = 0;
		public int getRecordsRead() {return providerRecordsRead;}
		public int getRecordsAccepted() {return providerRecordsAccepted;}
		public int getRecordsRejected() {return providerRecordsRejected;}
	}

}
