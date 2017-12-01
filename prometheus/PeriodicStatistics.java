

public class PeriodicStatistics {
	
	private String claimType;
	private Date beginDate;
	private Date endDate;
	private double noOfPatients;
	private double noOfRecords;
	private double totalAllowedAmount;
	
	public String getClaimType() {
		return claimType;
	}
	public void setClaimType(String claimType) {
		this.claimType = claimType;
	}
	public Date getBeginDate() {
		return beginDate;
	}
	public void setBeginDate(Date beginDate) {
		this.beginDate = beginDate;
	}
	public Date getEndDate() {
		return endDate;
	}
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	public double getNoOfPatients() {
		return noOfPatients;
	}
	public void setNoOfPatients(double noOfPatients) {
		this.noOfPatients = noOfPatients;
	}
	public double getNoOfRecords() {
		return noOfRecords;
	}
	public void setNoOfRecords(double noOfRecords) {
		this.noOfRecords = noOfRecords;
	}
	public double getTotalAllowedAmount() {
		return totalAllowedAmount;
	}
	public void setTotalAllowedAmount(double totalAllowedAmount) {
		this.totalAllowedAmount = totalAllowedAmount;
	}

}
