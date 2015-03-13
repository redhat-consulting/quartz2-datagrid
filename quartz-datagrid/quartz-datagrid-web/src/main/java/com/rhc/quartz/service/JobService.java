package com.rhc.quartz.service;

import java.util.Date;
import java.util.List;

import javax.ejb.Stateless;
import javax.inject.Inject;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rhc.quartz.dao.JobDAO;
import com.rhc.quartz.entity.JobRecord;
import com.rhc.quartz.job.FireJob;
import com.rhc.quartz.scheduler.SchedulerContainer;

@Stateless
public class JobService {
	private static final Logger LOG = LoggerFactory.getLogger(JobService.class);

	@Inject
	private SchedulerContainer schedulerContainer;
	
	public void createJob(String message) {
		Date scheduleTime = new Date(new Date().getTime() + (1000 * 5));
		JobRecord jobRecord = new JobRecord();
		jobRecord.setMessage(message);
		jobRecord.setScheduledTime(scheduleTime);
		jobRecord.setFired(false);
		JobDAO.INSTANCE.put(jobRecord);
		LOG.info("Created "+jobRecord.getMessage()+" job #" + jobRecord.getId());
		
		
	    JobDetail jobDetail = JobBuilder.newJob(FireJob.class)
	    		.usingJobData(FireJob.JOB_RECORD_ID_KEY, jobRecord.getId())
	        .build();

	    Trigger trigger = TriggerBuilder.newTrigger().startAt(scheduleTime)           
	        .build();
	    try {
	    	LOG.info("Scheduling job #" + jobRecord.getId());
	    	schedulerContainer.getScheduler().scheduleJob(jobDetail, trigger);
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public List<JobRecord> getJobRecords() {
		return JobDAO.INSTANCE.getAll();
	}
}
