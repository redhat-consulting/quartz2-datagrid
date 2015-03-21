package com.rhc.quartz.infinispan.dao;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.quartz.TriggerKey;

import com.rhc.quartz.infinispan.wrapper.TriggerWrapper;

public interface TriggerDAO {

	TriggerWrapper get(TriggerKey key);
	
	List<TriggerWrapper> getAll();

	void add(TriggerWrapper tw);

	Map<TriggerKey, TriggerWrapper> getGroup(String group);

	void putGroup(String group, Map<TriggerKey, TriggerWrapper> grpMap);

	void put(TriggerKey key, TriggerWrapper tw);

	Set<String> getPausedGroups();

	TriggerWrapper remove(TriggerKey key);

	void removeGroup(String group);

	int numberOfTriggers();
	
	Set<Entry<String, Map<TriggerKey, TriggerWrapper>>> getAllGroups();

	Collection<? extends String> getAllGroupNames();

	boolean removeTimeTrigger(TriggerWrapper tw);
	void addTimeTrigger(TriggerWrapper tw);
	void addAllTimeTriggers(Set<TriggerWrapper> tws);
	
	//its a set
	boolean addPausedTriggerGroup(String name);
	
	void unpauseTriggerGroup(String name);
	
	int timeTriggersSize();
	TriggerWrapper getFirstTimeTrigger();


}
