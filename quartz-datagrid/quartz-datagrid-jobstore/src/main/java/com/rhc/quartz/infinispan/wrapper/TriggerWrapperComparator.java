package com.rhc.quartz.infinispan.wrapper;

import java.util.Comparator;

import org.quartz.Trigger.TriggerTimeComparator;

public class TriggerWrapperComparator implements Comparator<TriggerWrapper>, java.io.Serializable {
  
    private static final long serialVersionUID = 8809557142191514261L;

    TriggerTimeComparator ttc = new TriggerTimeComparator();
    
    public int compare(TriggerWrapper trig1, TriggerWrapper trig2) {
        return ttc.compare(trig1.trigger, trig2.trigger);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof TriggerWrapperComparator);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}