package com.rhc.quartz.infinispan.dao.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rhc.quartz.infinispan.dao.TriggerDAO;
import com.rhc.quartz.infinispan.wrapper.TriggerWrapper;
import com.rhc.quartz.infinispan.wrapper.TriggerWrapperComparator;

public class HashMapTriggerDAO implements TriggerDAO{
	private static final Logger LOG = LoggerFactory.getLogger(HashMapTriggerDAO.class);
	    protected HashMap<TriggerKey, TriggerWrapper> triggersByKey = new HashMap<TriggerKey, TriggerWrapper>(1000);
	    protected HashMap<String, Map<TriggerKey, TriggerWrapper>> triggersByGroup = new HashMap<String, Map<TriggerKey, TriggerWrapper>>(25);
	    protected TreeSet<TriggerWrapper> timeTriggers = new TreeSet<TriggerWrapper>(new TriggerWrapperComparator());
	    protected ArrayList<TriggerWrapper> triggers = new ArrayList<TriggerWrapper>(1000);
	    protected HashSet<String> pausedTriggerGroups = new HashSet<String>();

	public TriggerWrapper get(TriggerKey key) {
		LOG.info("Get " + key);
		return triggersByKey.get(key);
	}

	public List<TriggerWrapper> getAll() {
		LOG.info("Get all");
		return triggers;
	}

	public void add(TriggerWrapper tw) {
		LOG.info("add " + tw);
		triggers.add(tw);
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
		return pausedTriggerGroups;
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
		return timeTriggers.remove(tw);
	}

	public void addTimeTrigger(TriggerWrapper tw) {
		LOG.info("Add time " + tw);
		timeTriggers.add(tw);
	}

	public void addAllTimeTriggers(Set<TriggerWrapper> tws) {
		timeTriggers.addAll(tws);
	}

	public boolean addPausedTriggerGroup(String name) {
		return pausedTriggerGroups.add(name);
	}

	public void unpauseTriggerGroup(String name) {
		pausedTriggerGroups.remove(name);
	}

	public int timeTriggersSize() {
		LOG.info("Get time size");
		return timeTriggers.size();
	}

	public TriggerWrapper getFirstTimeTrigger() {
		return timeTriggers.first();
	}

}
