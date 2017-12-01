



public class ProviderDataFromPebtf extends ProviderDataAbstract implements ProviderDataInterface  {


	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ProviderDataFromPebtf(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
		bPreserveQuotes = true;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ProviderDataFromPebtf () {
		this.errMgr = null;
		super.sDelimiter = this.sDelimiter;
		bPreserveQuotes = true;
	}

	
	
	protected boolean doMap(int col_ix, String col_value) {
		
		boolean bResult = true;

		ColumnFinder col_clue = col_index.get(col_ix);
		if (col_clue == null)
			{}
		else
		{
			if (col_value != null  && !(col_value.isEmpty())) {
				stats.column_stats.get(col_clue.col_name.toUpperCase()).filled_count++;
			}
			/*
			if (col_clue.col_name.toUpperCase().startsWith("PROVIDER_SPECIALTY_"))
	    	{
				if (col_value != null  && !(col_value.trim().isEmpty()))
					provider.addSpecialty(col_value);
	    	}
	    	else
	    	*/
			if (col_value != null  && !(col_value.trim().isEmpty()))
			switch (col_clue.col_name) {
			case "BPRVN":
				provider.setProvider_id(getColumnValue("P_SOURCE").substring(0, 1) + col_value);
				break;
			case "PROVNPI":
				provider.setNPI(col_value);
				break;
			case "BPRONAM":
				provider.setProvider_name(col_value);
				break;
			case "BPROVZIP":
				provider.setZipcode(col_value);
				break;
			case "BPROVSPC":
				if (col_value != null  && !(col_value.trim().isEmpty()))
					provider.addSpecialty(lookupSpecialty(col_value));
				break;
			default:
				break;	
			}

		}
		
		return bResult;
	}

	/**
	 * check for a header
	 * return true indicates first line is data (not a header)
	 * PEBTF does not provide headers
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {
		
		stats.fileClass = "Providers";
		
		int i=0;
		for (String col : cols_to_find) {
			InputStatistics.StatEntry e = stats.new StatEntry();
			e.bExpected = true;
	    	e.col_name = col;
		

	    	stats.column_stats.put(col.toUpperCase(), e);
	    	col_index.put(i, new ColumnFinder(i, col));

		    i++;
		}  
		
		return true;		 // means header is never found, so process the first record as data
		
	}



	// providers come from claim file
	String [] cols_to_find = {
			"P_SOURCE",
			"P_TYPPLN",
			"P_CLMTYP",
			"P_BENCDE",
			"PLAN",
			"P_AR",
			"CONID",
			"MHICN",
			"SUBZP1",
			"GRPNBR",
			"PATSSN",
			"PLSTNAME",
			"PFSTNAME",
			"PATSLT",
			"PATREL",
			"P_PATGNDR",
			"PATSTATUS",
			"PATDOB",
			"CLAIMNBR",
			"ITMNBR",
			"CLAIMTYP",
			"CSRCCD",
			"DOS",
			"DISDT",
			"RECDT",
			"P_PDDT",
			"AMTPD ",
			"P_INVDAT",
			"BPRVN",
			"BPRONAM",
			"BPROVZIP",
			"BPROVTYP",
			"BPROVSPC",
			"PROVCLAS",
			"PLACESVC",
			"TYPESVC",
			"DIAGCD1",
			"DIAGCD2",
			"DIAGCD3",
			"DIAGCD4",
			"DIAGCD5",
			"DIAGCD6",
			"DIAGCDID ",
			"P_DIAGCDID",
			"POA1",
			"POA2",
			"POA3",
			"POA4",
			"POA5",
			"POA6",
			"PROCCD",
			"PROCTYID ",
			"P_PROCTYID",
			"PROCM1",
			"PROCM2",
			"NUMSVC",
			"SUBCHG",
			"P_CLMVAL",
			"P_FLXAMT",
			"P_CAPAMT",
			"CVRDCHG",
			"DEDUCT",
			"COINS",
			"COPAY",
			"COINSDY",
			"EXCMAX",
			"ACCFEE",
			"FEEIND",
			"ICENT1",
			"ICENT2",
			"P_NOTCVCD",
			"P_NOTCVAMT",
			"P_EXBENCD",
			"P_EXBENAMT",
			"P_COBCD1",
			"P_COBAMT1",
			"P_COBCD2",
			"P_COBAMT2",
			"P_CNTCD1",
			"P_CNTAMT1",
			"P_CNTCD21",
			"P_CNTAMT2",
			"P_OTHCD1",
			"P_OTHAMT1",
			"P_OTHCD2",
			"P_OTHAMT2",
			"P_DSCCD1",
			"P_DSCAMT1",
			"P_DSCCD2",
			"P_DSCAMT2",
			"DSPCD ",
			"P_REJADJCD",
			"P_REJADJTY",
			"CLREMRK1",
			"EMPLNAME",
			"EMPFNAME",
			"PAYSTS",
			"P_CRITCODE",
			"P_CRITSEQ",
			"P_CKMATCH",
			"P_CLMCONSQ",
			"P_CLMLINSQ",
			"P_SUBNO",
			"P_PERNO",
			"P_MBREDTC",
			"P_PLAN",
			"P_HLTHPL",
			"DOS",
			"P_PDDT",
			"EMPID",
			"EMPDOB",
			"EMPGNDER",
			"NOUNITS",
			"NOUNITS2",
			"PROCCD",
			"PROCPRM",
			"PROCM1",
			"PROCM2",
			"PROC2",
			"PROC3",
			"PROVTYPE",
			"PROVPAY",
			"DRGCODE",
			"ADMSNTYP",
			"PROVALWD",
			"PROVNPI",
			"PERFNPI",
			"FACLTYBIL",
			"REVCODE#",
			"REVCDEVAL",
			"Filler"
	};
	
	
	
	@Override
	protected void storeProvider () {
		providers.put(provider.getProvider_id(), provider);
	}
	
	
	private String lookupSpecialty(String sIn) {
		sSpc = sIn;
		if (getColumnValue("P_HLTHPL").trim().equals("HMK")) {
			for (int i=0; i < specialtyLookupHMK.length; i++) {
				if (sIn.equals(specialtyLookupHMK[i][0].replaceFirst("^0+(?!$)", "")) ) {
					sSpc = specialtyLookupHMK[i][1];
					break;
				}
			}
		}
		return sSpc;
	}
	private String sSpc;
	
	private String [][] specialtyLookupHMK = {
			{"0",	"99"},
			{"1",	"02"},
			{"2",	"14"},
			{"3",	"20"},
			{"4",	"24"},
			{"5",	"33"},
			{"6",	"16"},
			{"7",	"16"},
			{"8",	"16"},
			{"9",	"18"},
			{"10",	"04"},
			{"11",	"18"},
			{"12",	"28"},
			{"13",	"34"},
			{"14",	"11"},
			{"15",	"06"},
			{"16",	"10"},
			{"17",	"37"},
			{"18",	"13"},
			{"19",	"86"},
			{"20",	"05"},
			{"21",	"07"},
			{"22",	"30"},
			{"23",	"01"},
			{"24",	"08"},
			{"25",	"88"},
			{"26",	"22"},
			{"27",	"25"},
			{"28",	"99"},
			{"29",	"70"},
			{"30",	"03"},
			{"31",	"48"},
			{"32",	"19"},
			{"33",	"88"},
			{"34",	"88"},
			{"35",	"35"},
			{"36",	"88"},
			{"37",	"88"},
			{"38",	"88"},
			{"39",	"46"},
			{"40",	"82"},
			{"41",	"44"},
			{"42",	"90"},
			{"43",	"39"},
			{"44",	"29"},
			{"45",	"66"},
			{"46",	"36"},
			{"47",	"37"},
			{"48",	"84"},
			{"49",	"37"},
			{"50",	"02"},
			{"51",	"37"},
			{"52",	"92"},
			{"53",	"16"},
			{"54",	"53"},
			{"55",	"93"},
			{"56",	"42"},
			{"57",	"41"},
			{"58",	"88"},
			{"59",	"59"},
			{"60",	"64"},
			{"61",	"81"},
			{"62",	"62"},
			{"63",	"63"},
			{"64",	"47"},
			{"65",	"65"},
			{"66",	"98"},
			{"67",	"49"},
			{"68",	"88"},
			{"69",	"99"},
			{"70",	"88"},
			{"71",	"88"},
			{"72",	"88"},
			{"73",	"15"},
			{"74",	"67"},
			{"75",	"67"},
			{"76",	"88"},
			{"77",	"88"},
			{"78",	"88"},
			{"79",	"43"},
			{"80",	"A4"},
			{"81",	"80"},
			{"82",	"97"},
			{"83",	"58"},
			{"84",	"88"},
			{"85",	"77"},
			{"86",	"12"},
			{"87",	"54"},
			{"88",	"88"},
			{"89",	"76"},
			{"90",	"74"},
			{"91",	"38"},
			{"92",	"40"},
			{"93",	"88"},
			{"93",	"88"},
			{"94",	"88"},
			{"95",	"88"},
			{"96",	"89"},
			{"97",	"89"},
			{"98",	"88"},
			{"99",	"88"},
			{"100",	"88"},
			{"101",	"50"},
			{"102",	"86"},
			{"103",	"78"},
			{"104",	"79"},
			{"105",	"88"},
			{"106",	"85"},
			{"107",	"91"},
			{"108",	"94"},
			{"109",	"83"},
			{"110",	"88"},
			{"111",	"88"},
			{"112",	"88"},
			{"113",	"C0"},
			{"114",	"03"},
			{"115",	"37"},
			{"116",	"88"},
			{"117",	"37"},
			{"118",	"88"},
			{"119",	"26"},
			{"120",	"26"},
			{"121",	"23"},
			{"122",	"50"},
			{"123",	"88"},
			{"124",	"22"},
			{"125",	"88"},
			{"126",	"99"},
			{"127",	"88"},
			{"128",	"12"},
			{"129",	"25"},
			{"130",	"23"},
			{"132",	"86"},
			{"133",	"86"},
			{"134",	"88"},
			{"135",	"22"},
			{"136",	"88"},
			{"137",	"88"},
			{"138",	"37"},
			{"139",	"86"},
			{"140",	"04"},
			{"141",	"86"},
			{"141",	"86"},
			{"142",	"78"},
			{"143",	"93"},
			{"144",	"11"},
			{"145",	"37"},
			{"146",	"50"},
			{"147",	"04"},
			{"148",	"88"},
			{"149",	"88"},
			{"150",	"37"},
			{"151",	"37"},
			{"152",	"37"},
			{"153",	"37"},
			{"154",	"37"},
			{"155",	"37"},
			{"156",	"37"},
			{"157",	"37"},
			{"158",	"37"},
			{"159",	"37"},
			{"160",	"37"},
			{"161",	"88"},
			{"162",	"88"},
			{"163",	"88"},
			{"164",	"58"},
			{"165",	"16"},
			{"166",	"37"},
			{"167",	"37"},
			{"168",	"88"},
			{"169",	"88"},
			{"170",	"79"},
			{"171",	"84"},
			{"172",	"22"},
			{"173",	"22"},
			{"174",	"22"},
			{"175",	"88"},
			{"176",	"07"},
			{"177",	"03"},
			{"178",	"11"},
			{"179",	"86"},
			{"180",	"22"},
			{"181",	"22"},
			{"182",	"37"},
			{"183",	"30"},
			{"184",	"99"},
			{"185",	"46"},
			{"186",	"04"},
			{"187",	"48"},
			{"188",	"22"},
			{"189",	"77"},
			{"190",	"27"},
			{"191",	"22"},
			{"192",	"11"},
			{"193",	"22"},
			{"194",	"93"},
			{"195",	"37"},
			{"196",	"13"},
			{"197",	"22"},
			{"198",	"99"},
			{"199",	"36"},
			{"200",	"30"},
			{"201",	"16"},
			{"202",	"84"},
			{"203",	"90"},
			{"204",	"22"},
			{"205",	"85"},
			{"206",	"04"},
			{"207",	"04"},
			{"208",	"04"},
			{"209",	"72"},
			{"210",	"07"},
			{"211",	"04"},
			{"212",	"22"},
			{"213",	"30"},
			{"214",	"24"},
			{"215",	"48"},
			{"216",	"48"},
			{"217",	"48"},
			{"218",	"48"},
			{"219",	"48"},
			{"220",	"26"},
			{"221",	"25"},
			{"222",	"99"},
			{"223",	"25"},
			{"224",	"02"},
			{"225",	"33"},
			{"226",	"34"},
			{"227",	"94"},
			{"228",	"88"},
			{"229",	"33"},
			{"230",	"88"},
			{"231",	"88"},
			{"232",	"88"},
			{"233",	"99"},
			{"234",	"88"},
			{"235",	"88"},
			{"236",	"71"},
			{"237",	"88"},
			{"238",	"88"},
			{"239",	"88"},
			{"240",	"88"},
			{"241",	"96"},
			{"242",	"88"},
			{"243",	"88"},
			{"244",	"62"},
			{"245",	"62"},
			{"246",	"62"},
			{"247",	"62"},
			{"248",	"62"},
			{"249",	"62"},
			{"250",	"62"},
			{"251",	"88"},
			{"252",	"88"},
			{"253",	"88"},
			{"254",	"62"},
			{"255",	"62"},
			{"256",	"62"},
			{"257",	"62"},
			{"258",	"88"},
			{"259",	"88"},
			{"260",	"89"},
			{"261",	"37"},
			{"262",	"37"},
			{"263",	"20"},
			{"264",	"71"},
			{"265",	"88"},
			{"266",	"11"},
			{"267",	"88"},
			{"268",	"69"},
			{"269",	"99"},
			{"270",	"88"},
			{"271",	"71"},
			{"272",	"88"},
			{"273",	"72"},
			{"274",	"88"},
			{"275",	"07"},
			{"280",	"88"},
			{"281",	"88"},
			{"282",	"88"},
			{"283",	"50"},
			{"284",	"88"},
			{"285",	"88"},
			{"286",	"43"},
			{"287",	"88"},
			{"288",	"88"},
			{"289",	"88"},
			{"290",	"88"},
			{"291",	"38"},
			{"293",	"88"},
			{"294",	"88"},
			{"296",	"06"},
			{"297",	"88"},
			{"298",	"88"},
			{"299",	"80"},
			{"301",	"A0"},
			{"302",	"A0"},
			{"303",	"A0"},
			{"304",	"B4"},
			{"305",	"A0"},
			{"306",	"A0"},
			{"307",	"A0"},
			{"308",	"A0"},
			{"309",	"A0"},
			{"310",	"A0"},
			{"311",	"A0"},
			{"312",	"A1"},
			{"313",	"A1"},
			{"314",	"A4"},
			{"315",	"B4"},
			{"316",	"B4"},
			{"317",	"B4"},
			{"318",	"B4"},
			{"319",	"B4"},
			{"320",	"A0"},
			{"321",	"A0"},
			{"322",	"A1"},
			{"323",	"A1"},
			{"324",	"B4"},
			{"325",	"B4"},
			{"326",	"49"},
			{"329",	"49"},
			{"330",	"A0"},
			{"333",	"B4"},
			{"334",	"A0"},
			{"335",	"B4"},
			{"336",	"B4"},
			{"337",	"A0"},
			{"338",	"88"},
			{"339",	"59"},
			{"340",	"88"},
			{"341",	"A0"},
			{"342",	"88"},
			{"344",	"88"},
			{"345",	"A2"},
			{"346",	"88"},
			{"347",	"88"},
			{"348",	"A4"},
			{"350",	"88"},
			{"351",	"88"},
			{"352",	"88"},
			{"353",	"88"},
			{"354",	"88"},
			{"355",	"88"},
			{"356",	"88"},
			{"357",	"88"},
			{"359",	"88"},
			{"360",	"88"},
			{"361",	"88"},
			{"362",	"A4"},
			{"363",	"A4"},
			{"364",	"88"},
			{"365",	"88"},
			{"366",	"B4"},
			{"367",	"88"},
			{"368",	"A0"},
			{"369",	"47"},
			{"370",	"88"},
			{"371",	"88"},
			{"372",	"88"},
			{"373",	"88"},
			{"374",	"88"},
			{"375",	"88"},
			{"376",	"88"},
			{"377",	"88"},
			{"378",	"A4"},
			{"379",	"A1"},
			{"380",	"A1"},
			{"381",	"88"},
			{"382",	"47"},
			{"383",	"88"},
			{"384",	"88"},
			{"385",	"88"},
			{"518",	"48"},
			{"900",	"88"},
			{"901",	"88"},
			{"902",	"88"},
			{"904",	"88"},
			{"905",	"88"},
			{"906",	"88"},
			{"907",	"B4"},
			{"908",	"88"},
			{"909",	"88"},
			{"910",	"A0"},
			{"911",	"88"},
			{"912",	"88"},
			{"913",	"88"},
	};

	
	
	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ProviderDataFromPebtf instance = new ProviderDataFromPebtf();
		try {
			instance.addSourceFile(PROVIDER_FILE);
			instance.prepareAllProviders ();
		} catch (Throwable e) {
			{log.info(e);}	// this main is just for testing - I don't care
		}
		
		// Printing the claim service line list populated. 
		if (bDebug)
		{
			GenericOutputInterface goif = new GenericOutputCSV();
			//GenericOutputInterface goif = new GenericOutputSQL();
			Collection<Provider> it = instance.providers.values();
			for (Provider member : it) { 
				goif.write(member);
				log.info(member);
			}
			goif.close();
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " provider input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " provider input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " provider input records");
		//log.info("Dupes found: " + instance.dupesFound);
		
	}
	
	//public static final String PROVIDER_FILE = "C:\\input\\XG_Prov_11192014.txt";
	public static final String PROVIDER_FILE = "C:\\input\\PEBTF\\HCI_Hmk_LMORGAN_201412011711P1_First1000.CSV";
	
	private static boolean bDebug = true;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ProviderDataFromPebtf.class);
	

	@Override
	public ProviderInputCounts getCounts() {
		return counts;
	}
	
	@Override
	public List<String> getErrorMsgs() {
		return errors;
	}


	@Override
	public InputStatistics getInputStatistics() {
		return stats;
	}

}
