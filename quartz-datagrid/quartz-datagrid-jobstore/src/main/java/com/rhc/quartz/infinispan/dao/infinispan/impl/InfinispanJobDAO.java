package com.rhc.quartz.infinispan.dao.infinispan.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.quartz.JobKey;

import com.rhc.quartz.infinispan.dao.JobDAO;
import com.rhc.quartz.infinispan.wrapper.JobWrapper;

public class InfinispanJobDAO implements JobDAO{
	private static final String JOBS_BY_KEY = "jobsByKey";
	private static final String JOBS_BY_GROUP = "jobsByGroup"; 
	private static final String PAUSED_GROUPS = "pausedGroups";
	private static final String PAUSED_GROUPS_KEY = "key";
	private static final String BLOCKED_JOBS = "blockedJobs";
	private static final String BLOCKED_JOBS_KEY = "key";
	
	
	 protected Cache<JobKey, JobWrapper> jobsByKey = null;
	 protected Cache<String, Map<JobKey, JobWrapper>> jobsByGroup = null;
	 protected Cache<String, Set<String>> pausedJobGroups = null;
	 protected Cache<String, Set<JobKey>> blockedJobs = null;
	 
	 public InfinispanJobDAO(CacheContainer cacheManager){
		 jobsByKey = cacheManager.getCache(JOBS_BY_KEY);
		 jobsByGroup = cacheManager.getCache(JOBS_BY_GROUP);
		 
		 pausedJobGroups = cacheManager.getCache(PAUSED_GROUPS);
		 pausedJobGroups.put(PAUSED_GROUPS_KEY, new HashSet<String>());
		 
		 
		 blockedJobs = cacheManager.getCache(BLOCKED_JOBS);
		 blockedJobs.put(BLOCKED_JOBS_KEY, new HashSet<JobKey>());
	 }

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
		return pausedJobGroups.get(PAUSED_GROUPS_KEY);
	}

	public Set<JobKey> getBlockedJobs() {
		return blockedJobs.get(BLOCKED_JOBS_KEY);
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
		Set<String> groups = pausedJobGroups.get(PAUSED_GROUPS_KEY);
		return groups.add(name);
	}

	public void unpauseJobGroup(String name) {
		pausedJobGroups.remove(name);
	}

	public void unpauseAllJobGroups() {
		pausedJobGroups.clear();
	}

	public boolean isBlocked(JobKey jobKey) {
		Set<JobKey> jobs = blockedJobs.get(BLOCKED_JOBS_KEY);
		return jobs.contains(jobKey);
	}

	public void unblockJob(JobKey jobKey) {
		blockedJobs.remove(jobKey);
	}

	public void blockJob(JobKey jobKey) {
		Set<JobKey> jobs = blockedJobs.get(BLOCKED_JOBS_KEY);
		jobs.add(jobKey);
	}

	public boolean isGroupPaused(String name) {
		Set<String> groups = pausedJobGroups.get(PAUSED_GROUPS_KEY);
		return groups.contains(name);
	}

}
