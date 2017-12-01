

@Entity
@Table(name = "inputMap")
public class MapEntry {

	@Id
	@Column(name="mapName")
	String mapName;
	@Column(name="mapContents", columnDefinition="TEXT")
	String mapContents;
	
	public MapEntry () {
		
	}
	
	public MapEntry (String mapName, String mapContents) {
		this.mapName = mapName;
		this.mapContents = mapContents;
	}
	
	
	
	
	public String getMapName() {
		return mapName;
	}

	public String getMapContents() {
		return mapContents;
	}

	
	

}
