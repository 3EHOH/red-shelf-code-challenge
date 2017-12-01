


public class SerializedMetaData implements Serializable {

	private static final long serialVersionUID = 1L;

	public ArrayList<EpisodeMetaData> epList = new ArrayList<EpisodeMetaData>();
	
	public ArrayList<String> emCodeList = new ArrayList<String>();
	
	public MetaDataHeader mdh = new MetaDataHeader();
	
}
