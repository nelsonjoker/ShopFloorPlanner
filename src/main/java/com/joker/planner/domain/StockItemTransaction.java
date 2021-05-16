package com.joker.planner.domain;

import java.io.Serializable;

public class StockItemTransaction implements Serializable{

    private StockItem mItem;
    private float mQuantity;
    private String mSTU;

    public StockItem getItem() { return mItem; }
    public void setItem(StockItem it) { this.mItem = it; }
    public float getQuantity() { return mQuantity; }
    public void setQuantity(float q) { this.mQuantity = q; }
    public String getSTU() { return mSTU; }
    public void setSTU(String s) { this.mSTU = s; }

    
    @Override
    public String toString() {
    	return mItem.toString()+" x "+mQuantity;
    }

    
    
}
