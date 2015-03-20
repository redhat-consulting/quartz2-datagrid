package com.rhc.quartz.infinispan.dao.impl;

import java.util.Collection;
import java.util.HashMap;

import org.quartz.Calendar;

import com.rhc.quartz.infinispan.dao.CalendarDAO;

public class HashMapCalendarDAO implements CalendarDAO {
	protected HashMap<String, Calendar> calendarsByName = new HashMap<String, Calendar>(25);

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
