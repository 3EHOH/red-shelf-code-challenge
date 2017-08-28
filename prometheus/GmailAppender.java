





public class GmailAppender extends SMTPAppender {
	
	private boolean startTLS = false;
	Session session;
	
	public GmailAppender() {
		super();
	}

	@Override
	protected Session createSession() {
		
		setSendOnClose(true);
		
		System.out.println("Hello");
		System.out.println("host " + this.getSMTPHost());
		System.out.println("port " + this.getSMTPPort());
		
		Properties props = null;
		try {
			props = new Properties(System.getProperties());
		} catch (SecurityException ex) {
			props = new Properties();
		}
		String prefix = "mail.smtp";
		if (this.getSMTPProtocol() != null) {
			props.put("mail.transport.protocol", this.getSMTPProtocol());
			prefix = "mail." + this.getSMTPProtocol();
		}

		if (this.getSMTPHost() != null) 
			props.put(prefix + ".host", this.getSMTPHost());
		if (this.getSMTPPort() > 0)
			props.put(prefix + ".port", String.valueOf(this.getSMTPPort()));
		if (this.startTLS) {
			props.put("mail.smtp.starttls.enable", "true");
			System.out.println("tls on");
		}
		if (this.getSMTPPassword() != null && this.getSMTPUsername() != null) {
			props.put(prefix + ".auth", "true");
		}

		/*
		Authenticator auth = null;
		if (this.getSMTPPassword() != null && this.getSMTPUsername() != null) {
			props.put(prefix + ".auth", "true");
			auth = new Authenticator() {

				@Override
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(GmailAppender.this.getSMTPUsername(), GmailAppender.this.getSMTPPassword());
				}

			};
		}
		Session session = Session.getInstance(props, auth);
		if (this.getSMTPProtocol() != null)
			session.setProtocolForAddress("rfc822", this.getSMTPProtocol());
		if (this.getSMTPDebug())
			session.setDebug(this.getSMTPDebug());
			*/
		
		System.out.println(this.getSMTPUsername());
		System.out.println(this.getSMTPPassword());
		System.out.println(this.getFrom());
		System.out.println(this.getTo());
		
		// Setup mail server
		//props.setProperty("mail.smtp.host", this.getSMTPHost());
		//props.setProperty("mail.smtp.port", String.valueOf(this.getSMTPPort()));
		//props.setProperty("mail.smtp.starttls.enable", "true");
		//props.setProperty("mail.smtp.auth", "true");
		
		// Get the default Session object.
		session = Session.getDefaultInstance(props,
				new Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(GmailAppender.this.getSMTPUsername(), GmailAppender.this.getSMTPPassword());
					}
			  	});

/*
		try{
			// Create a default MimeMessage object.
			MimeMessage message = new MimeMessage(session);

			// Set From: header field of the header.
			message.setFrom(new InternetAddress(this.getFrom()));

			// Set To: header field of the header.
			message.addRecipient(Message.RecipientType.TO,
					new InternetAddress(this.getTo()));

			// Set Subject: header field
			message.setSubject(this.getSubject());

			// Now set the actual message
			message.setText("test");

			// Send message
			Transport.send(message);
			System.out.println("Sent message successfully....");
		}catch (MessagingException mex) {
			mex.printStackTrace();
		}
		*/
	    
		
		return session;
	
	}
	
	@Override
	protected void sendBuffer() {
		try{
			// Create a default MimeMessage object.
			MimeMessage message = new MimeMessage(session);

			// Set From: header field of the header.
			message.setFrom(new InternetAddress(this.getFrom()));

			// Set To: header field of the header.
			message.addRecipient(Message.RecipientType.TO,
					new InternetAddress(this.getTo()));

			// Set Subject: header field
			message.setSubject(this.getSubject());

			// Now set the actual message
			message.setText( (String) cb.get().getMessage());

			// Send message
			Transport.send(message);
			//System.out.println("Sent message successfully....");
		}catch (MessagingException mex) {
			mex.printStackTrace();
		}
	}

	public boolean isStartTLS() {
		return this.startTLS;
	}

	public void setStartTLS(boolean startTLS) {
		this.startTLS = startTLS;
	}
	
	


}
