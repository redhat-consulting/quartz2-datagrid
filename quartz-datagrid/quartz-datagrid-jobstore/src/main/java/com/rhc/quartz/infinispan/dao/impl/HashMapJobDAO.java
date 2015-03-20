package com.rhc.quartz.infinispan.dao.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.quartz.JobKey;

import com.rhc.quartz.infinispan.dao.JobDAO;
import com.rhc.quartz.infinispan.wrapper.JobWrapper;

public class HashMapJobDAO implements JobDAO{
	 protected HashMap<JobKey, JobWrapper> jobsByKey = new HashMap<JobKey, JobWrapper>(1000);
	    protected HashMap<String, Map<JobKey, JobWrapper>> jobsByGroup = new HashMap<String, Map<JobKey, JobWrapper>>(25);
	    protected HashSet<String> pausedJobGroups = new HashSet<String>();
	    protected HashSet<JobKey> blockedJobs = new HashSet<JobKey>();

	public JobWrapper get(JobKey key) {
		return jobsByKey.get(key);
	}

	public void put(JobKey key, JobWrapper jw) {
		jobsByKey.put(key, jw);
	}

	public Map<JobKey, JobWrapper> getGroup(String string) {
		return jobsByGroup.get(string);
	}

	public void addGroup(String group, Map<JobKey, JobWrapper> grpMap) {
		jobsByGroup.put(group, grpMap);
	}

	public Object remove(JobKey jobKey) {
		return jobsByKey.remove(jobKey);
	}

	public void removeGroup(String group) {
		jobsByGroup.remove(group);
	}

	public Set<String> getPausedGroups() {
		return pausedJobGroups;
	}

	public Set<JobKey> getBlockedJobs() {
		return blockedJobs;
	}

	public int numberOfJobs() {
		return jobsByKey.size();
	}

	public Set<Entry<String, Map<JobKey, JobWrapper>>> getAllGroups() {
		return jobsByGroup.entrySet();
	}

	public Collection<? extends String> getAllGroupNames() {
		return jobsByGroup.keySet();
	}

	public boolean addPausedJobGroup(String name) {
		return pausedJobGroups.add(name);
	}

	public void unpauseJobGroup(String name) {
		pausedJobGroups.remove(name);
	}

	public void unpauseAllJobGroups() {
		pausedJobGroups.clear();
	}

	public boolean isBlocked(JobKey jobKey) {
		return blockedJobs.contains(jobKey);
	}

	public void unblockJob(JobKey jobKey) {
		blockedJobs.remove(jobKey);
	}

	public void blockJob(JobKey jobKey) {
		blockedJobs.add(jobKey);
	}

	public boolean isGroupPaused(String name) {
		return pausedJobGroups.contains(name);
	}

}
