package com.rhc.quartz.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;


import com.rhc.quartz.entity.JobRecord;

public class JobDAO {
	public static final JobDAO INSTANCE = new JobDAO();
	
	private JobDAO(){
		
	}
	
	private Map<Long, JobRecord> jobRecords = new ConcurrentHashMap<Long, JobRecord>();
	
	public void put(JobRecord newJobRecord) {
		newJobRecord.setId(new Random().nextLong());
		jobRecords.put(newJobRecord.getId(), newJobRecord);
	}
	
	public void setFired(Long jobId) {
		for (JobRecord job : jobRecords.values()) {
			if (job.getId().equals(jobId)) {
				job.setFired(true);
			}
		}
	}
	
	public List<JobRecord> getAll() {
		List<JobRecord> toReturn = new ArrayList<JobRecord>(jobRecords.size());
		for (JobRecord jobRecord : jobRecords.values()) {
			toReturn.add(jobRecord);
		}
		return toReturn;
	}
}
