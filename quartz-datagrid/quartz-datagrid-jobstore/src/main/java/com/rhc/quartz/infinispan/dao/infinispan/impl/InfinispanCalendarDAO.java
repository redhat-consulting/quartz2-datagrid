package com.rhc.quartz.infinispan.dao.infinispan.impl;

import java.util.Collection;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.quartz.Calendar;

import com.rhc.quartz.infinispan.dao.CalendarDAO;

public class InfinispanCalendarDAO implements CalendarDAO {
	private static final String CALENDARS_CACHE = "calendars";
	protected Cache<String, Calendar> calendarsByName = null;
	
	public InfinispanCalendarDAO(CacheContainer cacheManager) {
		calendarsByName = cacheManager.getCache(CALENDARS_CACHE);
	}

	public Calendar get(String name) {
		return calendarsByName.get(name);
	}

	public Object remove(String name) {
		return calendarsByName.remove(name);
	}

	public void put(String name, Calendar calendar) {
		calendarsByName.put(name, calendar);
	}

	public int numberOfCalendars() {
		return calendarsByName.size();
	}

	public Collection<? extends String> getAllCalendars() {
		return calendarsByName.keySet();
	}
}
