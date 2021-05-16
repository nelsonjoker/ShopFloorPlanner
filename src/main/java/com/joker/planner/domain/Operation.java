package com.joker.planner.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;



public class Operation implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4673114454032215933L;
	private static final int DISABLED_DURATION = 1;
	private long mStartTime;
	private int mDuration;
	private long mMaxStartTime;
	private int mResourceRequirement;
	private Resource mResource;
	private Resource mScheduledResource;
	private List<Resource> mResourceList;
	private WorkOrder mWorkOrder;
	private String mCode;
	private int mOPENUM;
	private int mNEXOPENUM;
	private boolean mIsMovable = true;
	private Operation mNextOperation;
	private List<Operation> mPreviousOperations;
	private double mExtqty;
	private double mCplqty;
	private boolean mDurationEnabled;
	
	public Operation(String code){
		this.mStartTime = System.currentTimeMillis()/1000;
		this.mMaxStartTime = this.mStartTime;
		this.mDuration = 0;
		this.mResourceRequirement = 0;
		this.mResource = null;
		this.mScheduledResource = null;
		this.mResourceList = null;
		this.mWorkOrder = null;
		this.mCode = code;
		this.mOPENUM = 0;
		this.mNEXOPENUM = 0;
		this.mIsMovable = true;
		this.mNextOperation = null;
		this.mPreviousOperations = new LinkedList<>();
		this.mExtqty = 0;
		this.mCplqty = 0;
		this.mDurationEnabled = true;
	}
	
	public Operation(Operation copy){
		this.mStartTime = copy.mStartTime;
		this.mMaxStartTime = copy.mMaxStartTime;
		this.mDuration = copy.mDuration;
		this.mResourceRequirement = copy.mResourceRequirement;
		this.mResource = copy.mResource;
		this.mScheduledResource = copy.mScheduledResource;
		this.mResourceList = new ArrayList<Resource>( copy.mResourceList);
		this.mWorkOrder = copy.mWorkOrder;
		this.mCode = copy.mCode;
		this.mOPENUM = copy.mOPENUM;
		this.mNEXOPENUM = copy.mNEXOPENUM;
		this.mIsMovable = copy.mIsMovable;
		this.mNextOperation = copy.mNextOperation;
		this.mPreviousOperations = new LinkedList<>( copy.mPreviousOperations);
		this.mExtqty = copy.mExtqty;
		this.mCplqty = copy.mCplqty;
		this.mDurationEnabled = copy.mDurationEnabled;
	}
	
	public void setDurationEnabled(boolean en) { mDurationEnabled = en;}
	public boolean isDurationEnabled() { return mDurationEnabled; }
	
	public String getCode() { return mCode; }
	public void setCode(String code) { mCode = code; }
	
	public int getOPENUM() { return mOPENUM; }
	public void setOPENUM(int o) { mOPENUM = o; }

	public int getNEXOPENUM() { return mNEXOPENUM; }
	public void setNEXOPENUM(int o) { mNEXOPENUM = o; }

	public Operation getNextOperation() { return mNextOperation; }
	public void setNextOperation(Operation o) {
		
			
		
		if(mNextOperation != null){
			List<Operation> ex = mNextOperation.getPreviousOperations();
			ex.remove(this);
		}
		mNextOperation = o; 
		//mNEXOPENUM = 0;
		if(o != null){
			//mNEXOPENUM = o.getOPENUM();
			o.getPreviousOperations().add(this);
			//testing
			Operation n = o;
			while((n = n.getNextOperation()) != null){
				if(n.equals(this)){
					//roll the f**k back
					n.setNextOperation(null);
					System.out.println("Recursive next operation");
				}
			}

			
		}
	
	}

	public List<Operation> getPreviousOperations() { return mPreviousOperations; }
	
	public WorkOrder getWorkOrder() {
		return mWorkOrder;
	}
	public void setWorkOrder(WorkOrder workOrder) {
		mWorkOrder = workOrder;
	}
	
    public Long getStartDate() { return mStartTime > 0 ? mStartTime : null; }
    public void setStartDate(Long startDate) {
    	assert(startDate >= 0);
    	mStartTime = startDate == null ? 0 : startDate; 
    	
    }
    //public Long getMaxStartDate() { return mMaxStartTime ; }
    //public void setMaxStartDate(Long startDate) {  mMaxStartTime = startDate; }
    
    /**
     * Gets how much time the operation will take in seconds
     * @return
     */
    public int getDuration() { return mDurationEnabled ? mDuration : DISABLED_DURATION; }
    /**
     * gets the raw duration value regardless of duration enabled flag
     * @return
     */
    public int getBaseDuration() { return mDuration; }
    
    /**
     * Sets the amount of time in seconds the operation requires to be completed
     * @param d
     */
    public void setDuration(int d) { mDuration = d; }
    public int getUnitDuration() { return (int) Math.max(1,  (mDuration / mExtqty) ); }
    
    /**
     * Sum of durations for this operation onwards 
     * @return
     */
    public int getOverallDuration(){
    	int d = getDuration();
    	if(getNextOperation() != null){
    		d += getNextOperation().getOverallDuration();
    	}
    	return d;
    }
    public long getOverallEndDate(){
    	if(getNextOperation() != null)
    		return getNextOperation().getOverallEndDate();
    	return getEndDate();
    }
    
    
    public long getEndDate() { return mStartTime + getDuration(); }
    
    
    public Resource getResource() { return mResource; }
	public void setResource(Resource resource) { mResource = resource; }
	
    public Resource getScheduledResource() { return mScheduledResource; }
	public void setScheduledResource(Resource resource) { mScheduledResource = resource; }

	/**
	 * The amount of resource required, usually equals duration
	 * @return
	 */
	//public int getResourceRequirement() { return mResourceRequirement; }
	/**
	 * The amount of resource required, usually equals duration
	 * @return
	 */
	public void setResourceRequirement(int resourceRequirement) { mResourceRequirement = resourceRequirement; }
	
	
	public void setExtqty(double v) { mExtqty = v ;}
	public double getExtqty() { return mExtqty; }
	public void setCplqty(double v) { mCplqty = v ;}
	public double getCplqty() { return mCplqty; }

	
	// ************************************************************************
    // Ranges
    // ************************************************************************
	public List<Resource> getResourceRange() { return mResourceList; }
	public void setResourceRange(List<Resource> rs) { mResourceList = rs; }

    
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
	
	@Override
	public boolean equals(Object other) {
		assert(other != null);
		if(other instanceof Operation){
			Operation o = (Operation) other;
			assert(mCode != null && o.mCode != null);
			return mCode.equals(o.mCode);
			//return Objects.equals(mCode, o.mCode);
		}
		return super.equals(other);
	}

	

	

	
	
}
