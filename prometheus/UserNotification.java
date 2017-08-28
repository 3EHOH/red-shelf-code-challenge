

public class UserNotification {
	
	public static void sendEmail (String message) {
		
		logger.info(message);
	}
	
	//private static Logger logger = Logger.getLogger("email");
	private static final Logger logger = Logger.getLogger(UserNotification.class);
	
	public static void main(String [] args)
	{
		//new UserNotification();
		//org.apache.log4j.net.SMTPAppender
		//logger.setAdditivity(false);
		UserNotification.sendEmail("yep");	
	}
	
}
