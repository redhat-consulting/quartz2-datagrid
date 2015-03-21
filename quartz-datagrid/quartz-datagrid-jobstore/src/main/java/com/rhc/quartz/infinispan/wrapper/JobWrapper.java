package com.rhc.quartz.infinispan.wrapper;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.quartz.JobDetail;
import org.quartz.JobKey;
 
@Indexed
public class JobWrapper {

	@IndexedEmbedded
	public GroupWrapper group;

	@Field
	public Boolean blocked = false;
	
    public JobKey key;

    public JobDetail jobDetail;

    public JobWrapper(JobDetail jobDetail) {
        this.jobDetail = jobDetail;
        key = jobDetail.getKey();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof JobWrapper) {
            JobWrapper jw = (JobWrapper) obj;
            if (jw.key.equals(this.key)) {
                return true;
            }
        }

        return false;
    }
    
    @Override
    public int hashCode() {
        return key.hashCode(); 
    }
}