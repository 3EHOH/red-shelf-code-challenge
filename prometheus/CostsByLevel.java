




public class CostsByLevel {
	
	public void writeCostLines(ArrayList<EpisodeShell> allEpisodes) throws IOException {
		
		String[] headerRow = new String[11];
		
		List<String[]> allRows = new ArrayList<String[]>();
		
		headerRow[0] = "master_episode_id";
		headerRow[1] = "level";
		headerRow[2] = "claim_type";
		headerRow[3] = "amt_split";
		headerRow[4] = "amt_unsplit";
		headerRow[5] = "amt_split_T";
		headerRow[6] = "amt_unsplit_T";
		headerRow[7] = "amt_split_C";
		headerRow[8] = "amt_unsplit_C";
		headerRow[9] = "amt_split_TC";
		headerRow[10] = "amt_unsplit_TC";
		
		allRows.add(headerRow);
		
		String[] row = new String[11];
		
		List<String> clTypeList = new ArrayList<String>();
		clTypeList.add("CL");
		clTypeList.add("IP");
		clTypeList.add("OP");
		clTypeList.add("PB");
		clTypeList.add("RX");
		
		String sFileName = FileNamer.getFileName("costByLevel") + ".csv";
		log.info("Creating " + sFileName );
		
		
		for (int i=1; i<6; i++) {
			String si = "" + i;
			for (String ty : clTypeList) {
		
				for (EpisodeShell es : allEpisodes) {
					
					if (es.isDropped()) {
						continue;
					}
					
					row = new String[11];
			
					row[0] = es.getEpisode_id() + "_" + es.getClaim_id() + "_" + es.getClaim().getClaim_line_id();
					row[1] = si;
					row[2] = ty;
					row[3] = es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit").toString();
					row[4] = es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit").toString();
					row[5] = es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_T").toString();
					row[6] = es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_T").toString();
					row[7] = es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_C").toString();
					row[8] = es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_C").toString();
					row[9] = es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_TC").toString();
					row[10] = es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_TC").toString();
					
					allRows.add(row);
				
				}
				
				CSVWriter writer =  new CSVWriter( new FileWriter( sFileName, true ) );
				
				writer.writeAll(allRows);
				
				writer.flush();
				writer.close();
			}
		}
	}
	
	private static org.apache.log4j.Logger log = Logger.getLogger(CostsByLevel.class);

}
