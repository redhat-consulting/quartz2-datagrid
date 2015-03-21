package com.rhc.quartz.infinispan.wrapper;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.bridge.builtin.BooleanBridge;

@Indexed
public class GroupWrapper  {

	@Field
	public String name;
	
	@Field
	@FieldBridge(impl = BooleanBridge.class)
	public Boolean paused;
}
