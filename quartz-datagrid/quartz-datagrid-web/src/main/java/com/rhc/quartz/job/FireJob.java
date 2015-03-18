package com.rhc.quartz.job;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rhc.quartz.dao.JobDAO;

public class FireJob implements Job {
	private static final Logger LOG = LoggerFactory.getLogger(FireJob.class);
	
	public static final String JOB_RECORD_ID_KEY = "job-record-id";

	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		Long id = dataMap.getLong(JOB_RECORD_ID_KEY);
		LOG.info("Firing job #" + id);
		JobDAO.INSTANCE.setFired(id);
	}
}
