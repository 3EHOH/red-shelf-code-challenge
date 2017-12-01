


public class DateRange {
	
	
	
	public static Date[] getFirstAndLastDatesFromServiceClaims (List<ClaimServLine> svcLines) {
		Date first_claim_date = null;
		Date last_claim_date = null;
		for (ClaimServLine clm : svcLines) { 
			// search for first and last claim dates
			try {
				if (first_claim_date == null || first_claim_date.after(clm.getBegin_date()))
					if (clm.getBegin_date() != null)
						first_claim_date = (Date) clm.getBegin_date().clone();
				//if (last_claim_date == null || last_claim_date.before(clm.getEnd_date()))
				if (last_claim_date == null || last_claim_date.before(clm.getBegin_date()))
					if (clm.getEnd_date() != null)
						last_claim_date = (Date) clm.getEnd_date().clone();
			}
			catch (NullPointerException e) {}
		}
		Date[] d = {first_claim_date, last_claim_date};
		return d;
	}
	
	public static Date[] getFirstAndLastDatesFromRxClaims (Collection<List<ClaimRx>> collection) {
		Date first_claim_date = null;
		Date last_claim_date = null;
		for (List<ClaimRx> clmList : collection) { 
			for (ClaimRx clm : clmList) {
			// search for first and last claim dates
				try {
					if (clm.getRx_fill_date() == null)
						continue;
					if (first_claim_date == null || first_claim_date.after(clm.getRx_fill_date()))
						first_claim_date = (Date) clm.getRx_fill_date().clone();
					if (last_claim_date == null || last_claim_date.before(clm.getRx_fill_date()))
						last_claim_date = (Date) clm.getRx_fill_date().clone();
				}
				catch (NullPointerException e) {}
			}
		}
		Date[] d = {first_claim_date, last_claim_date};
		return d;
	}
	
	
	

}
