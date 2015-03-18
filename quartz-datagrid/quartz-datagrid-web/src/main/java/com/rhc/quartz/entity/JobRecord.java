package com.rhc.quartz.entity;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "job-record")
@Entity
public class JobRecord {
	
	@Id
	@GeneratedValue
	private Long id;
	private String message;
	private Boolean fired;
	private Date scheduledTime;

	@XmlElement
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	@XmlElement
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	
	@XmlElement
	public Boolean getFired() {
		return fired;
	}
	public void setFired(Boolean fired) {
		this.fired = fired;
	}
	
	@XmlElement
	public Date getScheduledTime() {
		return scheduledTime;
	}
	public void setScheduledTime(Date scheduledTime) {
		this.scheduledTime = scheduledTime;
	}
}
