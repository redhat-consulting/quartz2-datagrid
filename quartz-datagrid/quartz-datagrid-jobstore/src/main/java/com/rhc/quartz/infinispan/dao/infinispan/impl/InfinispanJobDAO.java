package com.rhc.quartz.infinispan.dao.infinispan.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.quartz.JobKey;

import com.rhc.quartz.infinispan.dao.JobDAO;
import com.rhc.quartz.infinispan.wrapper.GroupWrapper;
import com.rhc.quartz.infinispan.wrapper.JobWrapper;

public class InfinispanJobDAO implements JobDAO{
	private static final String JOBS_BY_KEY = "jobsByKey";
	private static final String JOBS_BY_GROUP = "jobsByGroup"; 
	private static final String BLOCKED_JOBS = "blockedJobs";
	private static final String BLOCKED_JOBS_KEY = "key";
	
	protected final SearchManager groupSearchManager;

	protected Cache<JobKey, JobWrapper> jobsByKey = null;
	protected Cache<String, Map<JobKey, JobWrapper>> jobsByGroup = null;
	protected Cache<String, Set<JobKey>> blockedJobs = null;
	 
	 public InfinispanJobDAO(CacheContainer cacheManager){
		 jobsByKey = cacheManager.getCache(JOBS_BY_KEY);
		 jobsByGroup = cacheManager.getCache(JOBS_BY_GROUP);
		 
		 blockedJobs = cacheManager.getCache(BLOCKED_JOBS);
		 blockedJobs.put(BLOCKED_JOBS_KEY, new HashSet<JobKey>());
		 
		 Cache<String, GroupWrapper> cache = cacheManager.getCache();
		 groupSearchManager = Search.getSearchManager(cache);
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
			CacheQuery query = getJobsByPausedStatus(Boolean.TRUE);
			Set<String> groups = new HashSet<String>();
			for(Object gw : query) {
				String group = ((JobWrapper)gw).group.name;
				groups.add(group);
			}
			return groups;
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

		
		public void pauseJobGroup(String name) {
			// find all
			CacheQuery query = getJobsByName(name);
			if(query.iterator().hasNext()) {
				JobWrapper job = (JobWrapper)query.iterator().next();
				job.group.paused = true;
				
				//update the job
				this.jobsByKey.put(job.key, job);
			}
		}

		
		public void unpauseJobGroup(String name) {
			// find all
			CacheQuery query = getJobsByName(name);
			if(query.iterator().hasNext()) {
				JobWrapper job = (JobWrapper)query.iterator().next();
				job.group.paused = false;
				
				//update the job
				this.jobsByKey.put(job.key, job);
			}
		}

		public void unpauseAllJobGroups() {
			CacheQuery query = getJobsByPausedStatus(Boolean.TRUE);
			for(Object gw : query) {
				JobWrapper job = ((JobWrapper)gw);
				job.group.paused = false;

				//update the job
				this.jobsByKey.put(job.key, job);
			}
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

		public boolean isGroupPaused(String jobName) {
			// find all
			CacheQuery query = getJobsByName(jobName);
			if(query.iterator().hasNext()) {
				JobWrapper job = (JobWrapper)query.iterator().next();
				return job.group.paused;
			}
			return false;
		}
		
		
		protected CacheQuery getJobsByPausedStatus(Boolean jobPaused) {
			if(jobPaused == null) {
				throw new IllegalArgumentException("Exception");
			}
			String pausedVal = Boolean.toString(jobPaused).toLowerCase();
			
			// find all
			QueryBuilder qb = groupSearchManager.buildQueryBuilderForClass(JobWrapper.class).get();

			org.apache.lucene.search.Query luceneQuery =
					qb.bool()
					.must(qb.keyword().onField("group.paused").matching(pausedVal).createQuery())
				    .createQuery();
			CacheQuery query = groupSearchManager.getQuery(luceneQuery, JobWrapper.class);
			
			return query;
		}
		
		protected CacheQuery getJobsByName(String jobName) {
			// find all
			QueryBuilder qb = groupSearchManager.buildQueryBuilderForClass(JobWrapper.class).get();

			org.apache.lucene.search.Query luceneQuery =
					qb.bool()
					.must(qb.keyword().onField("group.name").matching(jobName).createQuery())
				    .createQuery();
			CacheQuery query = groupSearchManager.getQuery(luceneQuery, JobWrapper.class);
			
			return query;
		}
	}
