package com.joker.planner.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.optaplanner.core.api.domain.entity.PlanningEntity;
import org.optaplanner.core.api.domain.valuerange.CountableValueRange;
import org.optaplanner.core.api.domain.valuerange.ValueRangeFactory;
import org.optaplanner.core.api.domain.valuerange.ValueRangeProvider;
import org.optaplanner.core.api.domain.variable.PlanningVariable;

import com.joker.planner.solver.ResourceUsageStrengthWeightFactory;
import com.joker.planner.solver.OperationStartDateValueRange;
import com.joker.planner.solver.StartDateStrengthComparator;
import com.joker.planner.solver.filter.MovableOperationFilter;

@PlanningEntity(movableEntitySelectionFilter = MovableOperationFilter.class)
public class Operation implements Serializable{

	private long mStartTime;
	private int mDuration;
	private int mResourceRequirement;
	private Resource mResource;
	private List<Resource> mResourceList;
	private WorkOrder mWorkOrder;
	private String mCode;
	private int mOPENUM;
	private boolean mIsMovable = true;
	
	public Operation(){
		this.mStartTime = System.currentTimeMillis()/1000;
		this.mDuration = 0;
		this.mResourceRequirement = 0;
		this.mResource = null;
		this.mResourceList = null;
		this.mWorkOrder = null;
		this.mCode = null;
		this.mOPENUM = 0;
		this.mIsMovable = true;
	}
	
	public Operation(Operation copy){
		this.mStartTime = copy.mStartTime;
		this.mDuration = copy.mDuration;
		this.mResourceRequirement = copy.mResourceRequirement;
		this.mResource = copy.mResource;
		this.mResourceList = new ArrayList<>( copy.mResourceList);
		this.mWorkOrder = copy.mWorkOrder;
		this.mCode = copy.mCode;
		this.mOPENUM = copy.mOPENUM;
		this.mIsMovable = copy.mIsMovable;
	}
	
	
	public String getCode() { return mCode; }
	public void setCode(String code) { mCode = code; }
	
	public int getOPENUM() { return mOPENUM; }
	public void setOPENUM(int o) { mOPENUM = o; }
	
	public WorkOrder getWorkOrder() {
		return mWorkOrder;
	}
	public void setWorkOrder(WorkOrder workOrder) {
		mWorkOrder = workOrder;
	}
	
	//@PlanningVariable(valueRangeProviderRefs = {"startDateRange"})
	@PlanningVariable(valueRangeProviderRefs = {"startDateRange"}, strengthComparatorClass = StartDateStrengthComparator.class)
    public Long getStartDate() { return mStartTime > 0 ? mStartTime : null; }
    public void setStartDate(Long startDate) {
    	assert(startDate >= 0);
    	mStartTime = startDate == null ? 0 : startDate; 
    	
    }
    
    /**
     * Gets how much time the operation will take in seconds
     * @return
     */
    public int getDuration() { return mDuration; }
    /**
     * Sets the amount of time in seconds the operation requires to be completed
     * @param d
     */
    public void setDuration(int d) { mDuration = d; }
    
    public long getEndDate() { return mStartTime + mDuration; }
    
    
    //@PlanningVariable(valueRangeProviderRefs = {"resourceRange"})
    @PlanningVariable(valueRangeProviderRefs = {"resourceRange"}, strengthWeightFactoryClass = ResourceUsageStrengthWeightFactory.class)
    public Resource getResource() { return mResource; }
	public void setResource(Resource resource) { 
		mResource = resource; 
	}
	

	/**
	 * The amount of resource required, usually equals duration
	 * @return
	 */
	public int getResourceRequirement() { return mResourceRequirement; }
	/**
	 * The amount of resource required, usually equals duration
	 * @return
	 */
	public void setResourceRequirement(int resourceRequirement) { mResourceRequirement = resourceRequirement; }
	

	
	// ************************************************************************
    // Ranges
    // ************************************************************************

    @ValueRangeProvider(id = "resourceRange")
	public List<Resource> getResourceRange() { return mResourceList; }
	public void setResourceRange(List<Resource> rs) { mResourceList = rs; }

	@ValueRangeProvider(id = "startDateRange")
    public CountableValueRange<Long> getStartDateRange() {
    	long now = System.currentTimeMillis()/1000;
        return ValueRangeFactory.createLongValueRange(now - 30*24*3600, now + 60*24*3600);
    	
	}
	
	/*
	 @ValueRangeProvider(id = "startDateRange")
	 public List<Long> getStartDateRange(){
		 List<Long> res = new ArrayList<Long>();
		 res.add(mStartTime - 1);
		 res.add(mStartTime + 1);
		 return res;
	 }
	*/
	/*
    @ValueRangeProvider(id = "startDateRange")
    public List<Long> getStartDateRange(){
    	List<Long> res = new ArrayList<Long>();
    	
    	long now = System.currentTimeMillis()/1000;
    	now /= 24*3600;
    	now *= 24*3600;	//now at midnight
    	now += 12*3600;	//now at noon
    	for(long d = -5; d < 5; d++){
    		res.add(now + d*24*3600);	//5 days before/ 5 after now at noon
    	}
    	long min = Long.MAX_VALUE;
    	long max = Long.MIN_VALUE;
    	for(Resource r : mResourceList){
    		
    		List<Operation> ops = r.getOperationList();
    		Collections.sort(ops, new Comparator<Operation>() {
	
				@Override
				public int compare(Operation o1, Operation o2) {
					return (int)(o1.getStartDate() - o2.getStartDate());
				}
				
			});
    		
    		Iterator<Operation> it = ops.iterator();
    		if(!it.hasNext())
    			continue;
    		
    		Operation prev = it.next();
    		Operation next = null;
    		
    		res.add(prev.getStartDate() - mDuration);
    		
    		while(it.hasNext()){
    			next = it.next();
    			assert(next.getStartDate() >= prev.getStartDate());
    			long lag = next.getStartDate() - prev.getEndDate();
    			if(lag >= mDuration){
    				res.add(prev.getEndDate());
    			}
    			
    			prev = next;
    		}
    		if(next != null)
    			res.add(next.getEndDate());
    		
    		
//    		for(Operation op : r.getOperationList()){
//    			
//    			 
//    			
//    			long v = op.getStartDate() - this.getDuration() ;
//    			res.add(v);
//    			//res.add(v+24*300); 
//    			//res.add(v-24*300);
//    			
//    			min = Math.min(v, min);
//    			max = Math.max(v, max);
//    				
//    		}
    		
    	}
    	
//    	if(min < Long.MIN_VALUE)
//    		res.add(min - 3*24*3600);	//3 days before
//    	if(max > Long.MAX_VALUE)
//    		res.add(max + 3*24*3600);	//3 days after
    	
    	return res;
    }
    */
    
	/*
	//@ValueRangeProvider(id = "delayRange")
    @ValueRangeProvider(id = "startDateRange")
    public CountableValueRange<Long> getStartDateRange() {
    	long now = (System.currentTimeMillis()/1000);
        //return ValueRangeFactory.createLongValueRange(now - 365*24*3600, now + 365*24*3600);
        
        
    	Long m = getWorkOrder().getExecutionGroupForcedEnd();
    	long max = m == null ?  mStartTime + 5*24*3600 : m;
    	//long max = m == null ?  now + 30*24*3600 : m;
    	m = getWorkOrder().getExecutionGroupForcedStart();
    	long min = m == null ? mStartTime - 15*24*3600 : m;
    	//long min = m == null ? now - 30*24*3600 : m;
    	
    	//if(getWorkOrder().getNextOrder() != null){
    	//	max = Math.min(max, getWorkOrder().getNextOrder().getFirstStart());
    	//}
    	
    	
    	min = Math.min(min, max - 15*24*3600);
    	
    	long step = 1;
    	max = (max/step)*step;
    	min = (min/step)*step;
    	
    	return ValueRangeFactory.createLongValueRange(min, max, step);
    	
        //return ValueRangeFactory.createLongValueRange(mStartTime - 5*24*3600, mStartTime + 1*24*3600);
    	//return new OperationStartDateValueRange(this, min, (int) (max - min));
    	
    }
    */
    /*
    @ValueRangeProvider(id = "startDateRange")
    public Set<Long> getStartDateRange() {
    	
    	assert(getResource().getOperationList().contains(this));
    	Set<Long> res = new HashSet<Long>();
    	
    	long now = workingInterval(System.currentTimeMillis()/1000);
    	res.add(now);
    	
    	now /= 24*3600;
    	now *= 24*3600;	//now at midnight
    	now += 12*3600;	//now at noon
    	for(long d = -5; d < 5; d++){
    		res.add(now + d*24*3600);	//5 days before/ 5 after now at noon
    	}
    	
    	List<Operation> ops = getResource().getOperationList();
    	for(Operation op : ops){
    		if(op == this)
    			continue;
    		long t = op.getEndDate();
    		res.add(workingInterval(t));
    		t = op.getStartDate();
    		res.add(workingInterval(t));
    		t = op.getStartDate() - getDuration();
    		res.add(workingInterval(t));
    	}
    	return res;
    	
    }    
    */
    private Long workingInterval(long input){
    	return input;
    }
    
    private Long workingInterval_(long input){
    	long t = input;
		long date = (t / (24*3600))*24*3600;
		long min = (long) (date + 8.5*3600);
		t = Math.max(min, t);
		return t;
    }
    
    /**
     * indicates if this operation can be replanned
     * @return
     */
	public boolean isMovable() {
		return mIsMovable;
	}
	public void setMovable(boolean m) { 
		mIsMovable = m; 
	}
	

	
	@Override
    public String toString() {
    	int duration = getDuration();
    	long start = getStartDate();
    	//s += String.format(" start = %d (%d + %d) duration = %d ", getPredecessorsDoneDate() + delay , getPredecessorsDoneDate(), delay, duration);
    	return String.format("%s start = %d duration = %d @%s ", mCode, start , duration, mResource);
    }
	
	

	
	
}
