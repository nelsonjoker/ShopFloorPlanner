package com.joker.planner.domain;

import java.io.Serializable;

public class StockItemProductionTransaction extends StockItemTransaction implements Serializable{

	private String mVCRNUMORI;
	public String getVCRNUMORI() { return mVCRNUMORI; }
	public void setVCRNUMORI(String n) { mVCRNUMORI = n; }
	private Integer mVCRLINORI;
	public Integer getVCRLINORI() { return mVCRLINORI; }
	public void setVCRLINORI(Integer lin) { mVCRLINORI = lin; }
	private String mVCR;
	public String getVCR() { return mVCR; }
	public void setVCR(String code) { mVCR = code; }
	
}
