
public class AssignedClaim {
	
	String category; // T, TC, C (typical, complication...)
	String rule; //ex: 1.1.1, the assignment rule applied for this assignment
	ClaimServLine claim; //the claim service line that was assigned .. NOTE: could be the triggering service line.
	String type; // is it standard claim CL or RX
	ClaimRx rxClaim; // the Rx claim line
	double allowed_amt;

	public AssignedClaim() {
		type="CL";
	}
	
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getRule() {
		return rule;
	}
	public void setRule(String rule) {
		this.rule = rule;
	}
	public ClaimServLine getClaim() {
		return claim;
	}
	public void setClaim(ClaimServLine claim) {
		this.claim = claim;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public ClaimRx getRxClaim() {
		return rxClaim;
	}
	public void setRxClaim(ClaimRx rxClaim) {
		this.rxClaim = rxClaim;
	}
	
}
