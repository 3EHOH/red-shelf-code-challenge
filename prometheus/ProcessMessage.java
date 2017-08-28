



@Entity
@Table(name = "processMessage")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/


public class ProcessMessage {
	
	
		// default constructor for Hibernate
		public ProcessMessage () {}
		
		public ProcessMessage (long jobUid, int sequence, String stepName, String message) {
			this.jobUid = jobUid;
			this.sequence = sequence;
			this.stepName = stepName;
			this.message = message;
			this.updated = new Date();
		}
		
		@Id
		@GeneratedValue(generator="hibernate-native")
		//@GeneratedValue(generator = "sequence-style-generator")
		@Basic(optional = false)
		@Column(name="uid")
		private long uid;
		/*
		@Id @GeneratedValue
		@Column(name = "uid")
		private long uid;
		*/
		
		private long jobUid;
		private int sequence;
		private String stepName;
		private String member_id;
		private String message;
		private String status;
		private Date updated;
		
		
		public long getUid() {
			return uid;
		}
		public void setUid(long uid) {
			this.uid = uid;
		}
		public long getJobUid() {
			return jobUid;
		}
		public void setJobUid(long jobUid) {
			this.jobUid = jobUid;
		}
		public int getSequence() {
			return sequence;
		}
		public void setSequence(int sequence) {
			this.sequence = sequence;
		}
		public String getStepName() {
			return stepName;
		}
		public void setStepName(String stepName) {
			this.stepName = stepName;
		}
		public String getDescription() {
			return message;
		}
		public void setDescription(String message) {
			this.message = message;
		}
		public String getStatus() {
			return status;
		}
		public void setStatus(String status) {
			this.status = status;
		}
		public Date getUpdated() {
			return updated;
		}

		public void setUpdated(Date updated) {
			this.updated = updated;
		}

		public String getMember_id() {
			return member_id;
		}

		public void setMember_id(String member_id) {
			this.member_id = member_id;
		}


}
