package com.rhc.quartz.infinispan.dao;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.quartz.JobKey;

import com.rhc.quartz.infinispan.wrapper.JobWrapper;

public interface JobDAO {

	JobWrapper get(JobKey key);
	void put(JobKey key, JobWrapper jw);
	
	
	Map<JobKey, JobWrapper> getGroup(String string);
	void addGroup(String group, Map<JobKey, JobWrapper> grpMap);
	Object remove(JobKey jobKey);
	void removeGroup(String group);
	Set<String> getPausedGroups();
	Set<JobKey> getBlockedJobs();
	int numberOfJobs();
	Set<Entry<String, Map<JobKey, JobWrapper>>> getAllGroups();
	Collection<? extends String> getAllGroupNames();

	
	void pauseJobGroup(String name);
	void unpauseJobGroup(String name);
	void unpauseAllJobGroups();
	boolean isBlocked(JobKey jobKey);
	void unblockJob(JobKey jobKey);
	void blockJob(JobKey jobKey);
	boolean isGroupPaused(String name);
}
