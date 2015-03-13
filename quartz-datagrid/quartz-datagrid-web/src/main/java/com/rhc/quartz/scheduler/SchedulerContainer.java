package com.rhc.quartz.scheduler;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SchedulerContainer {
	private static final Logger LOG = LoggerFactory.getLogger(SchedulerContainer.class);
	
	private Scheduler scheduler;
	
	public synchronized Scheduler getScheduler() {
		return scheduler;
	}
	
	@PostConstruct
	public void init() {
		try {
			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.start();
		} catch (SchedulerException e) {
			LOG.error("Could not initialize scheduler",e);
		}
	}
	
	@PreDestroy
	public void cleanUp() {
		if (scheduler != null) {
			try {
				scheduler.shutdown();
			} catch (SchedulerException e) {
				LOG.error("Could not shutdown scheduler", e);
			}
		}
	}
}
