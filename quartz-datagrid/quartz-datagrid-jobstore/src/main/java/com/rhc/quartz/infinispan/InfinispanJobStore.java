package com.rhc.quartz.infinispan;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfinispanJobStore implements JobStore{
	private static final Logger LOG = LoggerFactory.getLogger(InfinispanJobStore.class);

	public void initialize(ClassLoadHelper loadHelper,
			SchedulerSignaler signaler) throws SchedulerConfigException {
		LOG.info("Executing method initialize");
	}

	public void schedulerStarted() throws SchedulerException {
		LOG.info("Executing method : [schedulerStarted]");
	}

	public void schedulerPaused() {
		LOG.info("Executing method : [schedulerPaused]");
	}

	public void schedulerResumed() {
		LOG.info("Executing method : [schedulerResumed]");
	}

	public void shutdown() {
		LOG.info("Executing method : [shutdown]");
	}

	public boolean supportsPersistence() {
		LOG.info("Executing method : [supportsPersistence]");
		return false;
	}

	public long getEstimatedTimeToReleaseAndAcquireTrigger() {
		LOG.info("Executing method : [getEstimatedTimeToReleaseAndAcquireTrigger]");
		return 0;
	}

	public boolean isClustered() {
		LOG.info("Executing method : [isClustered]");
		return false;
	}

	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.info("Executing method : [storeJobAndTrigger]");
	}

	public void storeJob(JobDetail newJob, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.info("Executing method : [storeJob]");
	}

	public void storeJobsAndTriggers(
			Map<JobDetail, Set<? extends Trigger>> triggersAndJobs,
			boolean replace) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		LOG.info("Executing method : [storeJobsAndTriggers]");
	}

	public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
		LOG.info("Executing method : [removeJob]");
		return false;
	}

	public boolean removeJobs(List<JobKey> jobKeys)
			throws JobPersistenceException {
		LOG.info("Executing method : [removeJobs]");
		return false;
	}

	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		LOG.info("Executing method : [retrieveJob]");
		return null;
	}

	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.info("Executing method : [storeTrigger]");
	}

	public boolean removeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [removeTrigger]");
		return false;
	}

	public boolean removeTriggers(List<TriggerKey> triggerKeys)
			throws JobPersistenceException {
		LOG.info("Executing method : [removeTriggers]");
		return false;
	}

	public boolean replaceTrigger(TriggerKey triggerKey,
			OperableTrigger newTrigger) throws JobPersistenceException {
		LOG.info("Executing method : [replaceTrigger]");
		return false;
	}

	public OperableTrigger retrieveTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [retrieveTrigger]");
		return null;
	}

	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		LOG.info("Executing method : [checkExists]");
		return false;
	}

	public boolean checkExists(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [checkExists]");
		return false;
	}

	public void clearAllSchedulingData() throws JobPersistenceException {
		LOG.info("Executing method : [clearAllSchedulingData]");
	}

	public void storeCalendar(String name, Calendar calendar,
			boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.info("Executing method : [storeCalendar]");
	}

	public boolean removeCalendar(String calName)
			throws JobPersistenceException {
		LOG.info("Executing method : [removeCalendar]");
		return false;
	}

	public Calendar retrieveCalendar(String calName)
			throws JobPersistenceException {
		LOG.info("Executing method : [retrieveCalendar]");
		return null;
	}

	public int getNumberOfJobs() throws JobPersistenceException {
		LOG.info("Executing method : [getNumberOfJobs]");
		return 0;
	}

	public int getNumberOfTriggers() throws JobPersistenceException {
		LOG.info("Executing method : [getNumberOfTriggers]");
		return 0;
	}

	public int getNumberOfCalendars() throws JobPersistenceException {
		LOG.info("Executing method : [getNumberOfCalendars]");
		return 0;
	}

	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [getJobKeys]");
		return null;
	}

	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [getTriggerKeys]");
		return null;
	}

	public List<String> getJobGroupNames() throws JobPersistenceException {
		LOG.info("Executing method : [getJobGroupNames]");
		return null;
	}

	public List<String> getTriggerGroupNames() throws JobPersistenceException {
		LOG.info("Executing method : [getTriggerGroupNames]");
		return null;
	}

	public List<String> getCalendarNames() throws JobPersistenceException {
		LOG.info("Executing method : [getCalendarNames]");
		return null;
	}

	public List<OperableTrigger> getTriggersForJob(JobKey jobKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [getTriggersForJob]");
		return null;
	}

	public TriggerState getTriggerState(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [getTriggerState]");
		return null;
	}

	public void pauseTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [pauseTrigger]");
	}

	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [pauseTriggers]");
		return null;
	}

	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		LOG.info("Executing method : [pauseJob]");
	}

	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [pauseJobs]");
		return null;
	}

	public void resumeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [resumeTrigger]");
	}

	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [resumeTriggers]");
		return null;
	}

	public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
		LOG.info("Executing method : [getPausedTriggerGroups]");
		return null;
	}

	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		LOG.info("Executing method : [resumeJob]");
	}

	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [resumeJobs]");
		return null;
	}

	public void pauseAll() throws JobPersistenceException {
		LOG.info("Executing method : [pauseAll]");
	}

	public void resumeAll() throws JobPersistenceException {
		LOG.info("Executing method : [resumeAll]");
	}

	public List<OperableTrigger> acquireNextTriggers(long noLaterThan,
			int maxCount, long timeWindow) throws JobPersistenceException {
		LOG.info("Executing method : [acquireNextTriggers]");
		return null;
	}

	public void releaseAcquiredTrigger(OperableTrigger trigger) {
		LOG.info("Executing method : [releaseAcquiredTrigger]");
	}

	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
			throws JobPersistenceException {
		LOG.info("Executing method : [triggersFired]");
		return null;
	}

	public void triggeredJobComplete(OperableTrigger trigger,
			JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
		LOG.info("Executing method : [triggeredJobComplete]");
	}

	public void setInstanceId(String schedInstId) {
		LOG.info("Executing method : [setInstanceId]");
	}

	public void setInstanceName(String schedName) {
		LOG.info("Executing method : [setInstanceName]");
	}

	public void setThreadPoolSize(int poolSize) {
		LOG.info("Executing method : [setThreadPoolSize]");
	}

}
