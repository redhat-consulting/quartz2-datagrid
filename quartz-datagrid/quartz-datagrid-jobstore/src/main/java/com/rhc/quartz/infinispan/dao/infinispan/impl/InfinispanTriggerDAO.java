package com.rhc.quartz.infinispan.dao.infinispan.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.quartz.TriggerKey;

import com.rhc.quartz.infinispan.dao.TriggerDAO;
import com.rhc.quartz.infinispan.wrapper.TriggerWrapper;
import com.rhc.quartz.infinispan.wrapper.TriggerWrapperComparator;

public class InfinispanTriggerDAO implements TriggerDAO{
	private static final String TRIGGERS_BY_KEY = "triggersByKey";
	private static final String TRIGGERS_BY_GROUP = "triggersByGroup";
	private static final String TIME_TRIGGERS = "timeTriggers";
	private static final String TIME_TRIGGERS_KEY = "key";
	private static final String TRIGGERS = "triggers";
	private static final String TRIGGERS_KEY = "key";
	private static final String PAUSED_TRIGGER_GROUPS = "pausedTriggerGroups";
	private static final String PAUSED_TRIGGER_GROUPS_KEY = "key";
	protected Cache<TriggerKey, TriggerWrapper> triggersByKey = null;
	protected Cache<String, Map<TriggerKey, TriggerWrapper>> triggersByGroup = null;
	protected Cache<String, TreeSet<TriggerWrapper>> timeTriggers = null;
	protected Cache<String, List<TriggerWrapper>> triggers = null;
	protected Cache<String, Set<String>> pausedTriggerGroups = null;
	    
	    public InfinispanTriggerDAO(CacheContainer cacheManager) {
			 triggersByKey = cacheManager.getCache(TRIGGERS_BY_KEY);
			 triggersByGroup = cacheManager.getCache(TRIGGERS_BY_GROUP);
			 
			 
			 timeTriggers = cacheManager.getCache(TIME_TRIGGERS);
			 timeTriggers.put(TIME_TRIGGERS_KEY, new TreeSet<TriggerWrapper>(new TriggerWrapperComparator()));
			 
			 
			 triggers = cacheManager.getCache(TRIGGERS);
			 triggers.put(TRIGGERS_KEY, new ArrayList<TriggerWrapper>());
			 
			 
			 pausedTriggerGroups = cacheManager.getCache(PAUSED_TRIGGER_GROUPS);
			 pausedTriggerGroups.put(PAUSED_TRIGGER_GROUPS_KEY, new HashSet<String>());
	    }

	public TriggerWrapper get(TriggerKey key) {
		return triggersByKey.get(key);
	}

	public List<TriggerWrapper> getAll() {
		return triggers.get(TRIGGERS_KEY);
	}

	public void add(TriggerWrapper tw) {
		triggers.get(TRIGGERS_KEY).add(tw);
	}

	public Map<TriggerKey, TriggerWrapper> getGroup(String group) {
		return triggersByGroup.get(group);
	}

	public void putGroup(String group, Map<TriggerKey, TriggerWrapper> grpMap) {
		triggersByGroup.put(group, grpMap);
	}

	public void put(TriggerKey key, TriggerWrapper tw) {
		triggersByKey.put(key, tw);
	}

	public Set<String> getPausedGroups() {
		return pausedTriggerGroups.get(PAUSED_TRIGGER_GROUPS_KEY);
	}

	public TriggerWrapper remove(TriggerKey key) {
		return triggersByKey.remove(triggersByKey);
	}

	public void removeGroup(String group) {
		triggersByGroup.remove(group);
	}

	public int numberOfTriggers() {
		return triggersByKey.size();
	}

	public Set<Entry<String, Map<TriggerKey, TriggerWrapper>>> getAllGroups() {
		return triggersByGroup.entrySet();
	}

	public Collection<? extends String> getAllGroupNames() {
		return triggersByGroup.keySet();
	}

	public boolean removeTimeTrigger(TriggerWrapper tw) {
		return timeTriggers.get(TIME_TRIGGERS_KEY).remove(tw);
	}

	public void addTimeTrigger(TriggerWrapper tw) {
		timeTriggers.get(TIME_TRIGGERS_KEY).add(tw);
	}

	public void addAllTimeTriggers(Set<TriggerWrapper> tws) {
		timeTriggers.get(TIME_TRIGGERS_KEY).addAll(tws);
	}

	public boolean addPausedTriggerGroup(String name) {
		return pausedTriggerGroups.get(PAUSED_TRIGGER_GROUPS_KEY).add(name);
	}

	public void unpauseTriggerGroup(String name) {
		pausedTriggerGroups.remove(name);
	}

	public int timeTriggersSize() {
		return timeTriggers.size();
	}

	public TriggerWrapper getFirstTimeTrigger() {
		return timeTriggers.get(TIME_TRIGGERS_KEY).first();
	}

}
