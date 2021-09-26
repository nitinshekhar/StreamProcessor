package com.nitin.model;

import com.nitin.common.CommunicationMode;
import com.nitin.common.Status;

public class Notification {
	private String participantId;
	private String notificationType;
	private Status status;
	private CommunicationMode communicationMode;
	
	public Notification() {
		
	}
	
	public Notification (String id, String notificationType, Status status) {
		this.participantId = id;
		this.notificationType = notificationType;
		this.status = status;
	}

	public String getParticipantId() {
		return participantId;
	}

	public void setParticipantId(String id) {
		this.participantId = id;
	}

	public String getNotificationType() {
		return notificationType;
	}

	public void setNotificationType(String notificationType) {
		this.notificationType = notificationType;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public CommunicationMode getCommunicationMode() {
		return communicationMode;
	}

	public void setCommunicationMode(CommunicationMode communicationMode) {
		this.communicationMode = communicationMode;
	}
}
