


public class SendMail
{
   public static void main(String [] args)
   {    
      // Recipient's email ID needs to be mentioned.
      String to = "warren.mcguire@hci3.org";

      // Sender's email ID needs to be mentioned
      String from = "warren.mcguire@hci3.org";

      // Assuming you are sending email from localhost
      //String host = "localhost";
      String host = "smtp.gmail.com";
      String smtpPort = "587";

      // Get system properties
      Properties properties = System.getProperties();

      // Setup mail server
      properties.setProperty("mail.smtp.host", host);
      properties.setProperty("mail.smtp.port", smtpPort);
      properties.setProperty("mail.smtp.starttls.enable", "true");
      properties.setProperty("mail.smtp.auth", "true");
    
      // Get the default Session object.
      Session session = Session.getDefaultInstance(properties,
      new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication("warren.mcguire@hci3.org", "hci3bridges");
			}
		  });


      try{
         // Create a default MimeMessage object.
         MimeMessage message = new MimeMessage(session);

         // Set From: header field of the header.
         message.setFrom(new InternetAddress(from));

         // Set To: header field of the header.
         message.addRecipient(Message.RecipientType.TO,
                                  new InternetAddress(to));

         // Set Subject: header field
         message.setSubject("This is the Subject Line!");

         // Now set the actual message
         message.setText("This is actual message");

         // Send message
         Transport.send(message);
         System.out.println("Sent message successfully....");
      }catch (MessagingException mex) {
         mex.printStackTrace();
      }
   }
}
