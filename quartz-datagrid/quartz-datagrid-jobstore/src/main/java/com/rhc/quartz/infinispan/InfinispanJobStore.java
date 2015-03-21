package com.rhc.quartz.infinispan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
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
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rhc.quartz.infinispan.dao.CalendarDAO;
import com.rhc.quartz.infinispan.dao.JobDAO;
import com.rhc.quartz.infinispan.dao.TriggerDAO;
import com.rhc.quartz.infinispan.dao.infinispan.impl.InfinispanCalendarDAO;
import com.rhc.quartz.infinispan.dao.infinispan.impl.InfinispanJobDAO;
import com.rhc.quartz.infinispan.dao.infinispan.impl.InfinispanTriggerDAO;
import com.rhc.quartz.infinispan.wrapper.JobWrapper;
import com.rhc.quartz.infinispan.wrapper.TriggerWrapper;

public class InfinispanJobStore implements JobStore{
	private static final Logger LOG = LoggerFactory.getLogger(InfinispanJobStore.class);
	protected CalendarDAO calendarDAO = null;
	protected JobDAO jobDAO = null;
	protected TriggerDAO triggerDAO = null;
	protected long misfireThreshold = 5000l;
	protected SchedulerSignaler signaler;
	private Object lock = new Object();

	public void initialize(ClassLoadHelper loadHelper,
			SchedulerSignaler signaler) throws SchedulerConfigException {
		LOG.info("Executing method initialize");
		CacheContainer cacheManager;
		try {
			cacheManager = new DefaultCacheManager("cache-config.xml",true);
		} catch (IOException e) {
			LOG.error("could not use xml", e);
			cacheManager = new DefaultCacheManager();
		}
		calendarDAO = new InfinispanCalendarDAO(cacheManager);
		jobDAO = new InfinispanJobDAO(cacheManager);
		triggerDAO = new InfinispanTriggerDAO(cacheManager);
		LOG.info("Done Executing method initialize");
        this.signaler = signaler;
	}
	
	protected long getMisfireThreshold() {
		return misfireThreshold;
	}

	public void schedulerStarted() throws SchedulerException {
		LOG.info("Executing method : [schedulerStarted]");
		//Nothing to do
	}

	public void schedulerPaused() {
		LOG.info("Executing method : [schedulerPaused]");
		//Nothing to do
	}

	public void schedulerResumed() {
		LOG.info("Executing method : [schedulerResumed]");
		//Nothing to do
	}

	public void shutdown() {
		LOG.info("Executing method : [shutdown]");
		//Close cache manager
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
		storeJob(newJob, false);
        storeTrigger(newTrigger, false);
	}

	public void storeJob(JobDetail newJob, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.info("Executing method : [storeJob]");
		
		 JobWrapper jw = new JobWrapper((JobDetail)newJob.clone());

	        boolean repl = false;

	        synchronized (lock) {
	            if (jobDAO.get(jw.key) != null) {
	                if (!replaceExisting) {
	                    throw new ObjectAlreadyExistsException(newJob);
	                }
	                repl = true;
	            }

	            if (!repl) {
	                // get job group
	                Map<JobKey, JobWrapper> grpMap = jobDAO.getGroup(newJob.getKey().getGroup());
	                if (grpMap == null) {
	                    grpMap = new HashMap<JobKey, JobWrapper>(100);
	                    jobDAO.addGroup(newJob.getKey().getGroup(), grpMap);
	                }
	                // add to jobs by group
	                grpMap.put(newJob.getKey(), jw);
	                // add to jobs by FQN map
	                jobDAO.put(jw.key, jw);
	            } else {
	                // update job detail
	                JobWrapper orig = jobDAO.get(jw.key);
	                orig.jobDetail = jw.jobDetail; // already cloned
	            }
	        }
	}

	public void storeJobsAndTriggers(
			Map<JobDetail, Set<? extends Trigger>> triggersAndJobs,
			boolean replace) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		LOG.info("Executing method : [storeJobsAndTriggers]");
        synchronized (lock) {
            // make sure there are no collisions...
            if(!replace) {
                for(Entry<JobDetail, Set<? extends Trigger>> e: triggersAndJobs.entrySet()) {
                    if(checkExists(e.getKey().getKey()))
                        throw new ObjectAlreadyExistsException(e.getKey());
                    for(Trigger trigger: e.getValue()) {
                        if(checkExists(trigger.getKey()))
                            throw new ObjectAlreadyExistsException(trigger);
                    }
                }
            }
            // do bulk add...
            for(Entry<JobDetail, Set<? extends Trigger>> e: triggersAndJobs.entrySet()) {
                storeJob(e.getKey(), true);
                for(Trigger trigger: e.getValue()) {
                    storeTrigger((OperableTrigger) trigger, true);
                }
            }
        }
	}

	public boolean removeJob(JobKey jobKey) {
		LOG.info("Executing method : [removeJob]");

        boolean found = false;

        synchronized (lock) {
            List<OperableTrigger> triggersOfJob = getTriggersForJob(jobKey);
            for (OperableTrigger trig: triggersOfJob) {
                this.removeTrigger(trig.getKey());
                found = true;
            }
            
            found = (jobDAO.remove(jobKey) != null) | found;
            if (found) {

                Map<JobKey, JobWrapper> grpMap = jobDAO.getGroup(jobKey.getGroup());
                if (grpMap != null) {
                    grpMap.remove(jobKey);
                    if (grpMap.size() == 0) {
                        jobDAO.removeGroup(jobKey.getGroup());
                    }
                }
            }
        }

        return found;
	}

	public boolean removeJobs(List<JobKey> jobKeys)
			throws JobPersistenceException {
		LOG.info("Executing method : [removeJobs]");
		boolean allFound = true;

        synchronized (lock) {
            for(JobKey key: jobKeys)
                allFound = removeJob(key) && allFound;
        }

        return allFound;
	}

	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		LOG.info("Executing method : [retrieveJob]");
        synchronized(lock) {
            JobWrapper jw = jobDAO.get(jobKey);
            return (jw != null) ? (JobDetail)jw.jobDetail.clone() : null;
        }
	}

	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.info("Executing method : [storeTrigger]");
		TriggerWrapper tw = new TriggerWrapper((OperableTrigger)newTrigger.clone());

        synchronized (lock) {
            if (triggerDAO.get(tw.key) != null) {
                if (!replaceExisting) {
                    throw new ObjectAlreadyExistsException(newTrigger);
                }
    
                removeTrigger(newTrigger.getKey(), false);
            }
    
            if (retrieveJob(newTrigger.getJobKey()) == null) {
                throw new JobPersistenceException("The job ("
                        + newTrigger.getJobKey()
                        + ") referenced by the trigger does not exist.");
            }

            // add to triggers array
            triggerDAO.add(tw);
            // add to triggers by group
            Map<TriggerKey, TriggerWrapper> grpMap = triggerDAO.getGroup(newTrigger.getKey().getGroup());
            if (grpMap == null) {
                grpMap = new HashMap<TriggerKey, TriggerWrapper>(100);
                triggerDAO.putGroup(newTrigger.getKey().getGroup(), grpMap);
            }
            grpMap.put(newTrigger.getKey(), tw);
            // add to triggers by FQN map
            triggerDAO.put(tw.key, tw);

            Set<String> pausedTriggerGroups = triggerDAO.getPausedGroups();
            Set<String> pausedJobGroups = jobDAO.getPausedGroups();
            Set<JobKey> blockedJobs = jobDAO.getBlockedJobs();
            if (pausedTriggerGroups.contains(newTrigger.getKey().getGroup())
                    || pausedJobGroups.contains(newTrigger.getJobKey().getGroup())) {
                tw.state = TriggerWrapper.STATE_PAUSED;
                if (blockedJobs.contains(tw.jobKey)) {
                    tw.state = TriggerWrapper.STATE_PAUSED_BLOCKED;
                }
            } else if (blockedJobs.contains(tw.jobKey)) {
                tw.state = TriggerWrapper.STATE_BLOCKED;
            } else {
                triggerDAO.addTimeTrigger(tw);
            }
        }
	}

	public boolean removeTrigger(TriggerKey triggerKey) {
		LOG.info("Executing method : [removeTrigger]");
		return removeTrigger(triggerKey, true);
	}

	    
	    private boolean removeTrigger(TriggerKey key, boolean removeOrphanedJob) {

	        boolean found;

	        synchronized (lock) {
	            // remove from triggers by FQN map
	            found = (triggerDAO.remove(key) != null);
	            if (found) {
	                TriggerWrapper tw = null;
	                // remove from triggers by group
	                Map<TriggerKey, TriggerWrapper> grpMap = triggerDAO.getGroup(key.getGroup());
	                if (grpMap != null) {
	                    grpMap.remove(key);
	                    if (grpMap.size() == 0) {
	                    	triggerDAO.removeGroup(key.getGroup());
	                    }
	                }
	                // remove from triggers array
	                Iterator<TriggerWrapper> tgs = triggerDAO.getAll().iterator();
	                while (tgs.hasNext()) {
	                    tw = tgs.next();
	                    if (key.equals(tw.key)) {
	                        tgs.remove();
	                        break;
	                    }
	                }
	                triggerDAO.removeTimeTrigger(tw);

	                if (removeOrphanedJob) {
	                    JobWrapper jw = jobDAO.get(tw.jobKey);
	                    List<OperableTrigger> trigs = getTriggersForJob(tw.jobKey);
	                    if ((trigs == null || trigs.size() == 0) && !jw.jobDetail.isDurable()) {
	                        if (removeJob(jw.key)) {
	                            signaler.notifySchedulerListenersJobDeleted(jw.key);
	                        }
	                    }
	                }
	            }
	        }

	        return found;
	    }

	public boolean removeTriggers(List<TriggerKey> triggerKeys)
			throws JobPersistenceException {
		LOG.info("Executing method : [removeTriggers]");
        boolean allFound = true;

        synchronized (lock) {
            for(TriggerKey key: triggerKeys)
                allFound = removeTrigger(key) && allFound;
        }

        return allFound;
	}

	public boolean replaceTrigger(TriggerKey triggerKey,
			OperableTrigger newTrigger) throws JobPersistenceException {
		LOG.info("Executing method : [replaceTrigger]");
		boolean found;

        synchronized (lock) {
            // remove from triggers by FQN map
            TriggerWrapper tw = triggerDAO.remove(triggerKey);
            found = (tw != null);

            if (found) {

                if (!tw.getTrigger().getJobKey().equals(newTrigger.getJobKey())) {
                    throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
                }

                tw = null;
                // remove from triggers by group
                Map<TriggerKey, TriggerWrapper> grpMap = triggerDAO.getGroup(triggerKey.getGroup());
                if (grpMap != null) {
                    grpMap.remove(triggerKey);
                    if (grpMap.size() == 0) {
                    	triggerDAO.removeGroup(triggerKey.getGroup());
                    }
                }
                // remove from triggers array
                Iterator<TriggerWrapper> tgs = triggerDAO.getAll().iterator();
                while (tgs.hasNext()) {
                    tw = tgs.next();
                    if (triggerKey.equals(tw.key)) {
                        tgs.remove();
                        break;
                    }
                }
                triggerDAO.removeTimeTrigger(tw);

                try {
                    storeTrigger(newTrigger, false);
                } catch(JobPersistenceException jpe) {
                    storeTrigger(tw.getTrigger(), false); // put previous trigger back...
                    throw jpe;
                }
            }
        }

        return found;
	}

	public OperableTrigger retrieveTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [retrieveTrigger]");
        synchronized(lock) {
            TriggerWrapper tw = triggerDAO.get(triggerKey);
    
            return (tw != null) ? (OperableTrigger)tw.getTrigger().clone() : null;
        }
	}

	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		LOG.info("Executing method : [checkExists]");
        synchronized(lock) {
            JobWrapper jw = jobDAO.get(jobKey);
            return (jw != null);
        }
	}

	public boolean checkExists(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [checkExists]");
        synchronized(lock) {
            TriggerWrapper tw = triggerDAO.get(triggerKey);
    
            return (tw != null);
        }
	}

	public void clearAllSchedulingData() throws JobPersistenceException {
		LOG.info("Executing method : [clearAllSchedulingData]");
		
		 synchronized (lock) {
	            // unschedule jobs (delete triggers)
	            List<String> lst = getTriggerGroupNames();
	            for (String group: lst) {
	                Set<TriggerKey> keys = getTriggerKeys(GroupMatcher.triggerGroupEquals(group));
	                for (TriggerKey key: keys) {
	                    removeTrigger(key);
	                }
	            }
	            // delete jobs
	            lst = getJobGroupNames();
	            for (String group: lst) {
	                Set<JobKey> keys = getJobKeys(GroupMatcher.jobGroupEquals(group));
	                for (JobKey key: keys) {
	                    removeJob(key);
	                }
	            }
	            // delete calendars
	            lst = getCalendarNames();
	            for(String name: lst) {
	                removeCalendar(name);
	            }
	        }
	}

	public void storeCalendar(String name, Calendar calendar,
			boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.info("Executing method : [storeCalendar]");

        calendar = (Calendar) calendar.clone();
        
        synchronized (lock) {
    
            Object obj = calendarDAO.get(name);
    
            if (obj != null && !replaceExisting) {
                throw new ObjectAlreadyExistsException(
                    "Calendar with name '" + name + "' already exists.");
            } else if (obj != null) {
                calendarDAO.remove(name);
            }
    
            calendarDAO.put(name, calendar);
    
            if(obj != null && updateTriggers) {
                for (TriggerWrapper tw : getTriggerWrappersForCalendar(name)) {
                    OperableTrigger trig = tw.getTrigger();
                    boolean removed = triggerDAO.removeTimeTrigger(tw);

                    trig.updateWithNewCalendar(calendar, getMisfireThreshold());

                    if (removed) {
                    	triggerDAO.addTimeTrigger(tw);
                    }
                }
            }
        }
	}

	public boolean removeCalendar(String calName)
			throws JobPersistenceException {
		LOG.info("Executing method : [removeCalendar]");
	       int numRefs = 0;

	        synchronized (lock) {
	            for (TriggerWrapper trigger : triggerDAO.getAll()) {
	                OperableTrigger trigg = trigger.trigger;
	                if (trigg.getCalendarName() != null
	                        && trigg.getCalendarName().equals(calName)) {
	                    numRefs++;
	                }
	            }
	        }

	        if (numRefs > 0) {
	            throw new JobPersistenceException(
	                    "Calender cannot be removed if it referenced by a Trigger!");
	        }

	        return (calendarDAO.remove(calName) != null);
	}

	public Calendar retrieveCalendar(String calName) {
		LOG.info("Executing method : [retrieveCalendar]");
        synchronized (lock) {
            Calendar cal = calendarDAO.get(calName);
            if(cal != null)
                return (Calendar) cal.clone();
            return null;
        }
	}

	public int getNumberOfJobs() throws JobPersistenceException {
		LOG.info("Executing method : [getNumberOfJobs]");
        synchronized (lock) {
            return jobDAO.numberOfJobs();
        }
	}

	public int getNumberOfTriggers() throws JobPersistenceException {
		LOG.info("Executing method : [getNumberOfTriggers]");
        synchronized (lock) {
            return triggerDAO.numberOfTriggers();
        }
	}

	public int getNumberOfCalendars() throws JobPersistenceException {
		LOG.info("Executing method : [getNumberOfCalendars]");
        synchronized (lock) {
            return calendarDAO.numberOfCalendars();
        }
	}

	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [getJobKeys]");
        Set<JobKey> outList = null;
        synchronized (lock) {

            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String compareToValue = matcher.getCompareToValue();

            switch(operator) {
                case EQUALS:
                    Map<JobKey, JobWrapper> grpMap = jobDAO.getGroup(compareToValue);
                    if (grpMap != null) {
                        outList = new HashSet<JobKey>();

                        for (JobWrapper jw : grpMap.values()) {

                            if (jw != null) {
                                outList.add(jw.jobDetail.getKey());
                            }
                        }
                    }
                    break;

                default:
                    for (Map.Entry<String, Map<JobKey, JobWrapper>> entry : jobDAO.getAllGroups()) {
                        if(operator.evaluate(entry.getKey(), compareToValue) && entry.getValue() != null) {
                            if(outList == null) {
                                outList = new HashSet<JobKey>();
                            }
                            for (JobWrapper jobWrapper : entry.getValue().values()) {
                                if(jobWrapper != null) {
                                    outList.add(jobWrapper.jobDetail.getKey());
                                }
                            }
                        }
                    }
            }
        }

        return outList == null ? java.util.Collections.<JobKey>emptySet() : outList;
	}

	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [getTriggerKeys]");
	      Set<TriggerKey> outList = null;
	        synchronized (lock) {

	            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
	            String compareToValue = matcher.getCompareToValue();

	            switch(operator) {
	                case EQUALS:
	                    Map<TriggerKey, TriggerWrapper> grpMap = triggerDAO.getGroup(compareToValue);
	                    if (grpMap != null) {
	                        outList = new HashSet<TriggerKey>();

	                        for (TriggerWrapper tw : grpMap.values()) {

	                            if (tw != null) {
	                                outList.add(tw.trigger.getKey());
	                            }
	                        }
	                    }
	                    break;

	                default:
	                    for (Map.Entry<String, Map<TriggerKey, TriggerWrapper>> entry : triggerDAO.getAllGroups()) {
	                        if(operator.evaluate(entry.getKey(), compareToValue) && entry.getValue() != null) {
	                            if(outList == null) {
	                                outList = new HashSet<TriggerKey>();
	                            }
	                            for (TriggerWrapper triggerWrapper : entry.getValue().values()) {
	                                if(triggerWrapper != null) {
	                                    outList.add(triggerWrapper.trigger.getKey());
	                                }
	                            }
	                        }
	                    }
	            }
	        }

	        return outList == null ? Collections.<TriggerKey>emptySet() : outList;
	}

	public List<String> getJobGroupNames() throws JobPersistenceException {
		LOG.info("Executing method : [getJobGroupNames]");
        List<String> outList;

        synchronized (lock) {
            outList = new LinkedList<String>(jobDAO.getAllGroupNames());
        }

        return outList;
	}

	public List<String> getTriggerGroupNames() throws JobPersistenceException {
		LOG.info("Executing method : [getTriggerGroupNames]");
        LinkedList<String> outList;

        synchronized (lock) {
            outList = new LinkedList<String>(triggerDAO.getAllGroupNames());
        }

        return outList;
	}

	public List<String> getCalendarNames() throws JobPersistenceException {
		LOG.info("Executing method : [getCalendarNames]");
        synchronized(lock) {
            return new LinkedList<String>(calendarDAO.getAllCalendars());
        }
	}

	public List<OperableTrigger> getTriggersForJob(JobKey jobKey) {
		LOG.info("Executing method : [getTriggersForJob]");
        ArrayList<OperableTrigger> trigList = new ArrayList<OperableTrigger>();

        synchronized (lock) {
            for (TriggerWrapper tw : triggerDAO.getAll()) {
                if (tw.jobKey.equals(jobKey)) {
                    trigList.add((OperableTrigger) tw.trigger.clone());
                }
            }
        }

        return trigList;
	}

	public TriggerState getTriggerState(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [getTriggerState]");
		 synchronized(lock) {
	            TriggerWrapper tw = triggerDAO.get(triggerKey);
	            
	            if (tw == null) {
	                return TriggerState.NONE;
	            }
	    
	            if (tw.state == TriggerWrapper.STATE_COMPLETE) {
	                return TriggerState.COMPLETE;
	            }
	    
	            if (tw.state == TriggerWrapper.STATE_PAUSED) {
	                return TriggerState.PAUSED;
	            }
	    
	            if (tw.state == TriggerWrapper.STATE_PAUSED_BLOCKED) {
	                return TriggerState.PAUSED;
	            }
	    
	            if (tw.state == TriggerWrapper.STATE_BLOCKED) {
	                return TriggerState.BLOCKED;
	            }
	    
	            if (tw.state == TriggerWrapper.STATE_ERROR) {
	                return TriggerState.ERROR;
	            }
	    
	            return TriggerState.NORMAL;
	        }
	}

	public void pauseTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [pauseTrigger]");
		synchronized (lock) {
            TriggerWrapper tw = triggerDAO.get(triggerKey);
    
            // does the trigger exist?
            if (tw == null || tw.trigger == null) {
                return;
            }
    
            // if the trigger is "complete" pausing it does not make sense...
            if (tw.state == TriggerWrapper.STATE_COMPLETE) {
                return;
            }

            if(tw.state == TriggerWrapper.STATE_BLOCKED) {
                tw.state = TriggerWrapper.STATE_PAUSED_BLOCKED;
            } else {
                tw.state = TriggerWrapper.STATE_PAUSED;
            }

            triggerDAO.removeTimeTrigger(tw);
        }
	}

	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [pauseTriggers]");
		List<String> pausedGroups;
        synchronized (lock) {
            pausedGroups = new LinkedList<String>();

            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            switch (operator) {
                case EQUALS:
                    if(triggerDAO.addPausedTriggerGroup(matcher.getCompareToValue())) {
                        pausedGroups.add(matcher.getCompareToValue());
                    }
                    break;
                default :
                    for (String group : triggerDAO.getAllGroupNames()) {
                        if(operator.evaluate(group, matcher.getCompareToValue())) {
                            if(triggerDAO.addPausedTriggerGroup(matcher.getCompareToValue())) {
                                pausedGroups.add(group);
                            }
                        }
                    }
            }

            for (String pausedGroup : pausedGroups) {
                Set<TriggerKey> keys = getTriggerKeys(GroupMatcher.triggerGroupEquals(pausedGroup));

                for (TriggerKey key: keys) {
                    pauseTrigger(key);
                }
            }
        }

        return pausedGroups;
	}

	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		LOG.info("Executing method : [pauseJob]");
        synchronized (lock) {
            List<OperableTrigger> triggersOfJob = getTriggersForJob(jobKey);
            for (OperableTrigger trigger: triggersOfJob) {
                pauseTrigger(trigger.getKey());
            }
        }
	}

	public Collection<String> pauseJobs(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [pauseJobs]");
		 List<String> pausedGroups = new LinkedList<String>();
	        synchronized (lock) {

	            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
	            switch (operator) {
	                case EQUALS:
	                    if (jobDAO.addPausedJobGroup(matcher.getCompareToValue())) {
	                        pausedGroups.add(matcher.getCompareToValue());
	                    }
	                    break;
	                default :
	                    for (String group : jobDAO.getAllGroupNames()) {
	                        if(operator.evaluate(group, matcher.getCompareToValue())) {
	                            if (jobDAO.addPausedJobGroup(group)) {
	                                pausedGroups.add(group);
	                            }
	                        }
	                    }
	            }

	            for (String groupName : pausedGroups) {
	                for (JobKey jobKey: getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
	                    List<OperableTrigger> triggersOfJob = getTriggersForJob(jobKey);
	                    for (OperableTrigger trigger: triggersOfJob) {
	                        pauseTrigger(trigger.getKey());
	                    }
	                }
	            }
	        }

	        return pausedGroups;
	}

	public void resumeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.info("Executing method : [resumeTrigger]");
		synchronized (lock) {
            TriggerWrapper tw = triggerDAO.get(triggerKey);
    
            // does the trigger exist?
            if (tw == null || tw.trigger == null) {
                return;
            }
    
            OperableTrigger trig = tw.getTrigger();
    
            // if the trigger is not paused resuming it does not make sense...
            if (tw.state != TriggerWrapper.STATE_PAUSED &&
                    tw.state != TriggerWrapper.STATE_PAUSED_BLOCKED) {
                return;
            }

            if(jobDAO.isBlocked( trig.getJobKey() )) {
                tw.state = TriggerWrapper.STATE_BLOCKED;
            } else {
                tw.state = TriggerWrapper.STATE_WAITING;
            }

            applyMisfire(tw);

            if (tw.state == TriggerWrapper.STATE_WAITING) {
                triggerDAO.addTimeTrigger(tw);
            }
        }
	}

	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [resumeTriggers]");
		Set<String> groups = new HashSet<String>();

        synchronized (lock) {
            Set<TriggerKey> keys = getTriggerKeys(matcher);

            for (TriggerKey triggerKey: keys) {
                groups.add(triggerKey.getGroup());
                if(triggerDAO.get(triggerKey) != null) {
                    String jobGroup = triggerDAO.get(triggerKey).jobKey.getGroup();
                    if(jobDAO.isGroupPaused(jobGroup)) {
                        continue;
                    }
                }
                resumeTrigger(triggerKey);
            }
            for (String group : groups) {
                triggerDAO.unpauseTriggerGroup(group);
            }
        }

        return new ArrayList<String>(groups);
	}

	public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
		LOG.info("Executing method : [getPausedTriggerGroups]");
		return null;
	}

	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		LOG.info("Executing method : [resumeJob]");
		 synchronized (lock) {
	            List<OperableTrigger> triggersOfJob = getTriggersForJob(jobKey);
	            for (OperableTrigger trigger: triggersOfJob) {
	                resumeTrigger(trigger.getKey());
	            }
	        }
	}

	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		LOG.info("Executing method : [resumeJobs]");
		  Set<String> resumedGroups = new HashSet<String>();
	        synchronized (lock) {
	            Set<JobKey> keys = getJobKeys(matcher);

	            for (String pausedJobGroup : jobDAO.getPausedGroups()) {
	                if(matcher.getCompareWithOperator().evaluate(pausedJobGroup, matcher.getCompareToValue())) {
	                    resumedGroups.add(pausedJobGroup);
	                }
	            }

	            for (String resumedGroup : resumedGroups) {
	                jobDAO.unpauseJobGroup(resumedGroup);
	            }

	            for (JobKey key: keys) {
	                List<OperableTrigger> triggersOfJob = getTriggersForJob(key);
	                for (OperableTrigger trigger: triggersOfJob) {
	                    resumeTrigger(trigger.getKey());
	                }
	            }
	        }
	        return resumedGroups;
	}

	public void pauseAll() throws JobPersistenceException {
		LOG.info("Executing method : [pauseAll]");
        synchronized (lock) {
            List<String> names = getTriggerGroupNames();

            for (String name: names) {
                pauseTriggers(GroupMatcher.triggerGroupEquals(name));
            }
        }
	}

	public void resumeAll() throws JobPersistenceException {
		LOG.info("Executing method : [resumeAll]");
        synchronized (lock) {
            jobDAO.unpauseAllJobGroups();
            resumeTriggers(GroupMatcher.anyTriggerGroup());
        }
	}

	public List<OperableTrigger> acquireNextTriggers(long noLaterThan,
			int maxCount, long timeWindow) throws JobPersistenceException {
		LOG.info("Executing method : [acquireNextTriggers]");
		  synchronized (lock) {
	            List<OperableTrigger> result = new ArrayList<OperableTrigger>();
	            Set<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
	            Set<TriggerWrapper> excludedTriggers = new HashSet<TriggerWrapper>();
	            long firstAcquiredTriggerFireTime = 0;
	            
	            // return empty list if store has no triggers.
	            if (triggerDAO.timeTriggersSize() == 0)
	                return result;
	            
	            while (true) {
	                TriggerWrapper tw;

	                try {
	                    tw = triggerDAO.getFirstTimeTrigger();
	                    if (tw == null)
	                        break;
	                    triggerDAO.removeTimeTrigger(tw);
	                } catch (java.util.NoSuchElementException nsee) {
	                    break;
	                }

	                if (tw.trigger.getNextFireTime() == null) {
	                    continue;
	                }

	                if (applyMisfire(tw)) {
	                    if (tw.trigger.getNextFireTime() != null) {
	                    	triggerDAO.addTimeTrigger(tw);
	                    }
	                    continue;
	                }

	                if (tw.getTrigger().getNextFireTime().getTime() > noLaterThan + timeWindow) {
	                	triggerDAO.addTimeTrigger(tw);
	                    break;
	                }
	                
	                // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
	                // put it back into the timeTriggers set and continue to search for next trigger.
	                JobKey jobKey = tw.trigger.getJobKey();
	                JobDetail job = jobDAO.get(tw.trigger.getJobKey()).jobDetail;
	                if (job.isConcurrentExectionDisallowed()) {
	                    if (acquiredJobKeysForNoConcurrentExec.contains(jobKey)) {
	                        excludedTriggers.add(tw);
	                        continue; // go to next trigger in store.
	                    } else {
	                        acquiredJobKeysForNoConcurrentExec.add(jobKey);
	                    }
	                }

	                tw.state = TriggerWrapper.STATE_ACQUIRED;
	                tw.trigger.setFireInstanceId(getFiredTriggerRecordId());
	                OperableTrigger trig = (OperableTrigger) tw.trigger.clone();
	                result.add(trig);
	                if(firstAcquiredTriggerFireTime == 0)
	                    firstAcquiredTriggerFireTime = tw.trigger.getNextFireTime().getTime();

	                if (result.size() == maxCount)
	                    break;
	            }
	            
	            // If we did excluded triggers to prevent ACQUIRE state due to DisallowConcurrentExecution, we need to add them back to store.
	            if (excludedTriggers.size() > 0)
	                triggerDAO.addAllTimeTriggers(excludedTriggers);
	            return result;
	        }
	}

	public void releaseAcquiredTrigger(OperableTrigger trigger) {
		LOG.info("Executing method : [releaseAcquiredTrigger]");
		 synchronized (lock) {
	            TriggerWrapper tw = triggerDAO.get(trigger.getKey());
	            if (tw != null && tw.state == TriggerWrapper.STATE_ACQUIRED) {
	                tw.state = TriggerWrapper.STATE_WAITING;
	                triggerDAO.addTimeTrigger(tw);
	            }
	        }
	}

	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> firedTriggers)
			throws JobPersistenceException {
		LOG.info("Executing method : [triggersFired]");

        synchronized (lock) {
            List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>();

            for (OperableTrigger trigger : firedTriggers) {
                TriggerWrapper tw = triggerDAO.get(trigger.getKey());
                // was the trigger deleted since being acquired?
                if (tw == null || tw.trigger == null) {
                    continue;
                }
                // was the trigger completed, paused, blocked, etc. since being acquired?
                if (tw.state != TriggerWrapper.STATE_ACQUIRED) {
                    continue;
                }

                Calendar cal = null;
                if (tw.trigger.getCalendarName() != null) {
                    cal = retrieveCalendar(tw.trigger.getCalendarName());
                    if(cal == null)
                        continue;
                }
                Date prevFireTime = trigger.getPreviousFireTime();
                // in case trigger was replaced between acquiring and firing
                triggerDAO.removeTimeTrigger(tw);
                // call triggered on our copy, and the scheduler's copy
                tw.trigger.triggered(cal);
                trigger.triggered(cal);
                //tw.state = TriggerWrapper.STATE_EXECUTING;
                tw.state = TriggerWrapper.STATE_WAITING;

                TriggerFiredBundle bndle = new TriggerFiredBundle(retrieveJob(
                        tw.jobKey), trigger, cal,
                        false, new Date(), trigger.getPreviousFireTime(), prevFireTime,
                        trigger.getNextFireTime());

                JobDetail job = bndle.getJobDetail();

                if (job.isConcurrentExectionDisallowed()) {
                    ArrayList<TriggerWrapper> trigs = getTriggerWrappersForJob(job.getKey());
                    for (TriggerWrapper ttw : trigs) {
                        if (ttw.state == TriggerWrapper.STATE_WAITING) {
                            ttw.state = TriggerWrapper.STATE_BLOCKED;
                        }
                        if (ttw.state == TriggerWrapper.STATE_PAUSED) {
                            ttw.state = TriggerWrapper.STATE_PAUSED_BLOCKED;
                        }
                        triggerDAO.removeTimeTrigger(ttw);
                    }
                    jobDAO.blockJob(job.getKey());
                } else if (tw.trigger.getNextFireTime() != null) {
                    synchronized (lock) {
                    	triggerDAO.addTimeTrigger(tw);
                    }
                }

                results.add(new TriggerFiredResult(bndle));
            }
            return results;
        }
	}

	public void triggeredJobComplete(OperableTrigger trigger,
			JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
		LOG.info("Executing method : [triggeredJobComplete]");
		 synchronized (lock) {

	            JobWrapper jw = jobDAO.get(jobDetail.getKey());
	            TriggerWrapper tw = triggerDAO.get(trigger.getKey());

	            // It's possible that the job is null if:
	            //   1- it was deleted during execution
	            //   2- RAMJobStore is being used only for volatile jobs / triggers
	            //      from the JDBC job store
	            if (jw != null) {
	                JobDetail jd = jw.jobDetail;

	                if (jd.isPersistJobDataAfterExecution()) {
	                    JobDataMap newData = jobDetail.getJobDataMap();
	                    if (newData != null) {
	                        newData = (JobDataMap)newData.clone();
	                        newData.clearDirtyFlag();
	                    }
	                    jd = jd.getJobBuilder().setJobData(newData).build();
	                    jw.jobDetail = jd;
	                }
	                if (jd.isConcurrentExectionDisallowed()) {
	                    jobDAO.unblockJob(jd.getKey());
	                    ArrayList<TriggerWrapper> trigs = getTriggerWrappersForJob(jd.getKey());
	                    for(TriggerWrapper ttw : trigs) {
	                        if (ttw.state == TriggerWrapper.STATE_BLOCKED) {
	                            ttw.state = TriggerWrapper.STATE_WAITING;
	                            triggerDAO.addTimeTrigger(ttw);
	                        }
	                        if (ttw.state == TriggerWrapper.STATE_PAUSED_BLOCKED) {
	                            ttw.state = TriggerWrapper.STATE_PAUSED;
	                        }
	                    }
	                    signaler.signalSchedulingChange(0L);
	                }
	            } else { // even if it was deleted, there may be cleanup to do
	            	jobDAO.unblockJob(jobDetail.getKey());
	            }
	    
	            // check for trigger deleted during execution...
	            if (tw != null) {
	                if (triggerInstCode == CompletedExecutionInstruction.DELETE_TRIGGER) {
	                    
	                    if(trigger.getNextFireTime() == null) {
	                        // double check for possible reschedule within job 
	                        // execution, which would cancel the need to delete...
	                        if(tw.getTrigger().getNextFireTime() == null) {
	                            removeTrigger(trigger.getKey());
	                        }
	                    } else {
	                        removeTrigger(trigger.getKey());
	                        signaler.signalSchedulingChange(0L);
	                    }
	                } else if (triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
	                    tw.state = TriggerWrapper.STATE_COMPLETE;
	                    triggerDAO.removeTimeTrigger(tw);
	                    signaler.signalSchedulingChange(0L);
	                } else if(triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
	                    LOG.info("Trigger " + trigger.getKey() + " set to ERROR state.");
	                    tw.state = TriggerWrapper.STATE_ERROR;
	                    signaler.signalSchedulingChange(0L);
	                } else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
	                    LOG.info("All triggers of Job " 
	                            + trigger.getJobKey() + " set to ERROR state.");
	                    setAllTriggersOfJobToState(trigger.getJobKey(), TriggerWrapper.STATE_ERROR);
	                    signaler.signalSchedulingChange(0L);
	                } else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
	                    setAllTriggersOfJobToState(trigger.getJobKey(), TriggerWrapper.STATE_COMPLETE);
	                    signaler.signalSchedulingChange(0L);
	                }
	            }
	        }
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

	
	
	
	 protected boolean applyMisfire(TriggerWrapper tw) {

	        long misfireTime = System.currentTimeMillis();
	        if (getMisfireThreshold() > 0) {
	            misfireTime -= getMisfireThreshold();
	        }

	        Date tnft = tw.trigger.getNextFireTime();
	        if (tnft == null || tnft.getTime() > misfireTime 
	                || tw.trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) { 
	            return false; 
	        }

	        Calendar cal = null;
	        if (tw.trigger.getCalendarName() != null) {
	            cal = retrieveCalendar(tw.trigger.getCalendarName());
	        }

	        signaler.notifyTriggerListenersMisfired((OperableTrigger)tw.trigger.clone());

	        tw.trigger.updateAfterMisfire(cal);

	        if (tw.trigger.getNextFireTime() == null) {
	            tw.state = TriggerWrapper.STATE_COMPLETE;
	            signaler.notifySchedulerListenersFinalized(tw.trigger);
	            synchronized (lock) {
	            	triggerDAO.removeTimeTrigger(tw);
	            }
	        } else if (tnft.equals(tw.trigger.getNextFireTime())) {
	            return false;
	        }

	        return true;
	    }
	 
	    private static final AtomicLong ftrCtr = new AtomicLong(System.currentTimeMillis());

	    protected String getFiredTriggerRecordId() {
	        return String.valueOf(ftrCtr.incrementAndGet());
	    }
	    
	    protected ArrayList<TriggerWrapper> getTriggerWrappersForJob(JobKey jobKey) {
	        ArrayList<TriggerWrapper> trigList = new ArrayList<TriggerWrapper>();

	        synchronized (lock) {
	            for (TriggerWrapper trigger : triggerDAO.getAll()) {
	                if (trigger.jobKey.equals(jobKey)) {
	                    trigList.add(trigger);
	                }
	            }
	        }

	        return trigList;
	    }

	    protected ArrayList<TriggerWrapper> getTriggerWrappersForCalendar(String calName) {
	        ArrayList<TriggerWrapper> trigList = new ArrayList<TriggerWrapper>();

	        synchronized (lock) {
	            for (TriggerWrapper tw : triggerDAO.getAll()) {
	                String tcalName = tw.getTrigger().getCalendarName();
	                if (tcalName != null && tcalName.equals(calName)) {
	                    trigList.add(tw);
	                }
	            }
	        }

	        return trigList;
	    }
	    
	    protected void setAllTriggersOfJobToState(JobKey jobKey, int state) {
	        ArrayList<TriggerWrapper> tws = getTriggerWrappersForJob(jobKey);
	        for (TriggerWrapper tw : tws) {
	            tw.state = state;
	            if (state != TriggerWrapper.STATE_WAITING) {
	                triggerDAO.removeTimeTrigger(tw);
	            }
	        }
	    }
}
