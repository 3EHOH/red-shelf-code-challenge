



public class ClaimDataToCSV {

	
	public void writeClaimLines(ArrayList<EpisodeShell> allEpisodes) throws IOException {
		
		log.info("write claim lines started...");
		
		List<String[]> allRows = new ArrayList<String[]>();
		
		String sFileName = FileNamer.getFileName("claimData")  + ".csv";
		File f = new File(sFileName);
		if(!f.exists()) {
			allRows.add(headerRow);
		}
		
		for(EpisodeShell e : allEpisodes) {
			
			if (e.getAssigned_claims().size()>0) {
				for (AssignedClaim as : e.getAssigned_claims()) {
					allRows.add(buildRow(e, as));
				}
			}
		}
		
		log.info("write claim lines before writing");
		
		
		CSVWriter writer =  new CSVWriter( new FileWriter( sFileName, true ) );
		log.info("Creating " + sFileName );
		
		writer.writeAll(allRows);
		
		log.info("write claim lines after write, before flush");
		
		writer.flush();
		
		log.info("write claim lines after flush");
		
		writer.close();
		
		log.info("write claim lines after close");
	}
	
	
	CSVWriter writer;
	String[] row= new String[11];
	

	
	private String[] buildRow (EpisodeShell e, AssignedClaim as) {
		
		row = new String[14];
		
		row[0] = e.getMember_id();
			
		if (as.getType().equals("RX")) {
			row[1] = as.getRxClaim().getClaim_id();
			row[2] = "1";
			row[4] = "" + as.getRxClaim().getAssignedCount();
			row[8] = "" + as.getRxClaim().getAllowed_amt();
			row[11] = as.getRxClaim().getClaim_id();
				
		} else {
			row[1] = as.getClaim().getClaim_id();
			row[2] = as.getClaim().getClaim_line_id();
			row[4] = "" + as.getClaim().getAssignedCount();
			row[8] = "" + as.getClaim().getAllowed_amt();
			row[11] = as.getClaim().getMaster_claim_id();
		}
			
		row[3] = "true";
			
		row[5] = e.getEpisode_id();
		row[6] = e.getClaim_id();
		row[7] = e.getClaim().getClaim_line_id();
			
		row[9] = as.getRule();
		row[10] = as.getCategory();
		
		
		row[12] = e.getClaim().getMaster_claim_id();
		row[13] = e.getMaster_episode_id();
		
		return row;
		
	}

	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimDataToCSV.class);

	
	static String[] headerRow;
	static {
		headerRow = new String[14];
		headerRow[0] = "member_id";
		headerRow[1] = "claim_id";
		headerRow[2] = "claim_line_id";
		headerRow[3] = "isAssigned";
		headerRow[4] = "AssignedCount";
		headerRow[5] = "ass_to_episode_id";
		headerRow[6] = "ass_to_claim_id";
		headerRow[7] = "ass_to_claim_line_id";
		headerRow[8] = "allowed_amt";
		headerRow[9] = "rule";
		headerRow[10] = "ass_type";
		headerRow[11] = "master_claim_id";
		headerRow[12] = "ass_to_master_claim_id";
		headerRow[13] = "ass_to_master_episode_id";
	}
	

}
