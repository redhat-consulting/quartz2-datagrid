package com.rhc.quartz.infinispan.dao;

import java.util.Collection;

import org.quartz.Calendar;

public interface CalendarDAO {

	Calendar get(String name);

	Object remove(String name);

	void put(String name, Calendar calendar);

	int numberOfCalendars();

	Collection<? extends String> getAllCalendars();

}
