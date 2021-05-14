package pt.sotubo.planner.domain;

import java.io.Serializable;

public class StockItemProductionTransaction extends StockItemTransaction implements Serializable{

	private String mVCRNUMORI;
	public String getVCRNUMORI() { return mVCRNUMORI; }
	public void setVCRNUMORI(String n) { mVCRNUMORI = n; }
	
}
