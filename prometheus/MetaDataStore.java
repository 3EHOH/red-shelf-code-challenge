





@Entity
@Table(name="metaDataStore")

public class MetaDataStore {
	
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Basic(optional = false)
	@Column(name="id")
	private int id;
	private String sourceType;
	private String sourceName;
	private String version;
	private String clazz;
	private Date loadDate;
	@ColumnTransformer(read = "UNCOMPRESS(metaData)", write = "COMPRESS(?)")
	@Lob
	private Byte[] metaData;
	
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getSourceType() {
		return sourceType;
	}
	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}
	public String getSourceName() {
		return sourceName;
	}
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public Byte[] getMetaData() {
		return metaData;
	}
	public void setMetaData(Byte[] metaData) {
		this.metaData = metaData;
	}
	
	
	public Date getLoadDate() {
		return loadDate;
	}
	public void setLoadDate(Date loadDate) {
		this.loadDate = loadDate;
	}


	public String getClazz() {
		return clazz;
	}
	public void setClazz(String clazz) {
		this.clazz = clazz;
	}


	@Transient
	public static final String SOURCE_TYPE_FILE = "file";
	@Transient
	public static final String SOURCE_TYPE_DB = "db";
	

}
