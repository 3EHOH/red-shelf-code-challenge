
public interface ReportInterface {
	
	public void createValidationReports(AllDataInterface di);

	public void createPatientCostDistribution(AllDataInterface di);
	
	public void createGeneralDataSummary(AllDataInterface di);
	
	public void finalize(AllDataInterface di);

}
