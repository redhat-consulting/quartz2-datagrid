package com.rhc.quartz.infinispan.wrapper;

import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

public class TriggerWrapper {

    public final TriggerKey key;

    public final JobKey jobKey;

    public final OperableTrigger trigger;

    public int state = STATE_WAITING;

    public static final int STATE_WAITING = 0;

    public static final int STATE_ACQUIRED = 1;

    public static final int STATE_EXECUTING = 2;

    public static final int STATE_COMPLETE = 3;

    public static final int STATE_PAUSED = 4;

    public static final int STATE_BLOCKED = 5;

    public static final int STATE_PAUSED_BLOCKED = 6;

    public static final int STATE_ERROR = 7;
    
    public TriggerWrapper(OperableTrigger trigger) {
        if(trigger == null)
            throw new IllegalArgumentException("Trigger cannot be null!");
        this.trigger = trigger;
        key = trigger.getKey();
        this.jobKey = trigger.getJobKey();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TriggerWrapper) {
            TriggerWrapper tw = (TriggerWrapper) obj;
            if (tw.key.equals(this.key)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode() {
        return key.hashCode(); 
    }

    
    public OperableTrigger getTrigger() {
        return this.trigger;
    }
}