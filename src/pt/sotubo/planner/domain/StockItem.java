package com.joker.planner.domain;

import java.io.Serializable;

public class StockItem implements Comparable<StockItem>, Serializable{

	private String mReference;
	public String getReference() { return mReference; }
	public void setReference(String r) { mReference = r; }
	
	
	private float mInitialAmount;
	public float getInitialAmount() { return mInitialAmount; }
	public void setInitialAmount(float r) { 
		mInitialAmount = r; 
	}
	
	private int mReorderCode;
	/**
	 * 3 is for manufactured items
	 * @return
	 */
	public int getReocod() { return mReorderCode; }
	public void setReocod(int r) { mReorderCode = r; }

	private String mCategory;
	public String getCategory() { return mCategory; }
	public void setCategory(String r) { mCategory = r; }
	
	@Override
	public String toString() {
		return mReference;
	}
	
	
	@Override
	public boolean equals(Object obj) {
		StockItem other = (StockItem)obj;
		return this.mReference.equals(other.mReference);
	}
	@Override
	public int compareTo(StockItem o) {
		return o == null ? -1 : this.mReference.compareTo(o.mReference);
	}
}
