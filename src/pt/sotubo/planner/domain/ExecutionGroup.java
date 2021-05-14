package pt.sotubo.planner.domain;

import java.io.Serializable;

public class ExecutionGroup implements Comparable<ExecutionGroup>, Serializable{

	private String mCode;
	public String getCode() { return mCode; }
	public void setCode(String code) { mCode = code; }
	
	private Long mStartTime;
	public Long getStartTime() { return mStartTime; }
	public void setStartTime(Long t) { mStartTime = t; }
	
	private Long mEndTime;
	public Long getEndTime() { return mEndTime; }
	public void setEndTime(Long t) { mEndTime = t; }

	
	private boolean mIsManual;
	public boolean isManual() { return mIsManual; }
	public void setManual(boolean m) { mIsManual = m; }
	
	private int mUsageCount;
	//public int getUsageCount() { return mUsageCount; }
	//public void setUsageCount(int c) { mUsageCount = c ; }
	
	private double mWeight;
	public double getWeight() { return mWeight; }
	public void setWeight(double c) { mWeight = c ; }
	

	public ExecutionGroup(String code){
		mCode = code;
		mStartTime = null;
		mEndTime = null;
		mIsManual = false;
		mUsageCount = 0;
		mWeight = 1;
		
		
	}
	
	
	@Override
	public boolean equals(Object obj) {
		ExecutionGroup g = (ExecutionGroup) obj;
			return g.mCode.equals(this.mCode);
	}
	@Override
	public int compareTo(ExecutionGroup o) {
		return o == null ? -1 : this.mCode.compareTo(o.mCode);
	}
	
}
