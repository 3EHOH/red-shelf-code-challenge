


@Entity
@Table(name = "association")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
)
*/ 

public class AssociationTbl {
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="id")
	private long id;
	
	@Column(name = "parent_master_episode_id")
	private String parent_master_episode_id;
	
	@Column(name = "child_master_episode_id")
	private String child_master_episode_id;
	
	@Column(name = "association_type")
	private String association_type;
	
	@Column(name = "association_level")
	private int association_level;
	
	@Column(name = "association_count")
	private int association_count;
	
	@Column(name = "association_start_day")
	private String association_start_day;
	
	@Column(name = "association_end_day")
	private String association_end_day;
	
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	
	public String getParent_master_episode_id() {
		return parent_master_episode_id;
	}
	public void setParent_master_episode_id(String parent_master_episode_id) {
		this.parent_master_episode_id = parent_master_episode_id;
	}
	public String getChild_master_episode_id() {
		return child_master_episode_id;
	}
	public void setChild_master_episode_id(String child_master_episode_id) {
		this.child_master_episode_id = child_master_episode_id;
	}
	public String getAssociation_type() {
		return association_type;
	}
	public void setAssociation_type(String assocation_type) {
		this.association_type = assocation_type;
	}
	public int getAssociation_level() {
		return association_level;
	}
	public void setAssociation_level(int association_level) {
		this.association_level = association_level;
	}
	public int getAssociation_count() {
		return association_count;
	}
	public void setAssociation_count(int association_count) {
		this.association_count = association_count;
	}
	public String getAssociation_start_day() {
		return association_start_day;
	}
	public void setAssociation_start_day(String association_start_day) {
		this.association_start_day = association_start_day;
	}
	public String getAssociation_end_day() {
		return association_end_day;
	}
	public void setAssociation_end_day(String association_end_day) {
		this.association_end_day = association_end_day;
	}
	

}
