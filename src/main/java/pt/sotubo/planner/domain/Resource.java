package pt.sotubo.planner.domain;

import java.io.Serializable;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Resource implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public String mId;
	public String getId() { return mId; }
	public void setId(String id) { mId = id; }

	public int mInstance;
	public int getInstance() { return mInstance; }
	public void setInstance(int c) { mInstance = c; }
	
	public int mCapacity;
	public int getCapacity() { return mCapacity; }
	public void setCapacity(int c) { mCapacity = c; }

	public int[] mDailyCapacity;
	public int[] getDailyCapacity() { return mDailyCapacity; }
	public void setDailyCapacity(int[] c) { mDailyCapacity = c; }

	public String mCode;
	public String getCode() { return mCode; }
	public void setCode(String c) { mCode = c; }
	
	public String mWorkcenter;
	public String getWorkcenter() { return mWorkcenter; }
	public void setWorkcenter(String c) { mWorkcenter = c; }
	
	public boolean isRenewable() { return false; }
	
	private Set<Operation> mOperations;	
	public Set<Operation> getOperationList(){ 
		return mOperations; 
	}
	public void setOperationList(Set<Operation> ops ) {
		mOperations = ops; 
	}
	
	public long getGroupingHorizon(){
		long h;
		if("COR".equals(mWorkcenter)){
			h = 2*3600;
		}else{		
			switch(mCode){
				case "LINPINT":
					h = 1*24*3600;
					break;
				case "EXPE":	//disable grouping
					h = 0;
					break;
				case "MCORTE":	
					h = 3*3600;
					break;
				case "QUINA":	
					h = 3*24*3600;
					break;
				case "PANEL":	
					h = 2*3600;
					break;
				default:
					h = (long) (1*3600);
					break;
			}
		}
		return h;
	}
	
	/**
	 * Alleged economic lot expressed in seconds
	 * @return
	 */
	public long getGroupingEOQ(){
		long h;
		if("COR".equals(mWorkcenter)){
			h = (long)(0.5*3600);
		}else{		
			switch(mCode){
				case "LINPINT":
					h = 2*3600;
					break;
				case "EXPE":	//disable grouping
					h = 0;
					break;
				case "MCORTE":	
					h = (long)(1*3600);
					break;
				default:
					h = 0;
					break;
			}
		}
		return h;
	}
	
//	public long getGroupingHorizon(){
//		
//		if("LINPINT".equals(mCode)){
//			return 5*24*3600;
//		}
//		return 365*24*3600;
//	}
//	
//	/**
//	 * Alleged economic lot expressed in seconds
//	 * @return
//	 */
//	public long getGroupingEOQ(){
//		
//		if("LINPINT".equals(mCode)){
//			return 3*3600;
//		}
//		return 0; //full search for everyone else
//		
//	}
//	
	
	public Resource(){
		mId = "";
		mInstance = 0;
		mCapacity = 0;
		mDailyCapacity  = null;
		mCode  = null;
		mWorkcenter = null;
		mOperations = new HashSet<Operation>();
		//mOperations = new ArrayList<>();
		/*
		mOperations = new SortedList<Operation>(new Comparator<Operation>() {

			@Override
			public int compare(Operation o1, Operation o2) {
				return (int)(o1.getStartDate() - o2.getStartDate());
			}
			
		});
		*/
	}
	
	public Resource(Resource copy){
		this();
		mId = copy.mId;
		mInstance = copy.mInstance;
		mCapacity = copy.mCapacity;
		mDailyCapacity  = copy.mDailyCapacity.clone();
		mCode  = copy.mCode;
		mWorkcenter = copy.mWorkcenter;
		//mOperations = new HashSet<>(copy.mOperations);
		mOperations.addAll(copy.mOperations);
	}
	
	
	@Override
	public String toString() {
		return mWorkcenter + "/" + mCode + "#" + mInstance;
	}
	
	@Override
	public boolean equals(Object obj) {
		Resource other = (Resource) obj;
		return other.mCode.equals(this.mCode) && other.mWorkcenter.equals(this.mWorkcenter) && other.mInstance == this.mInstance;
	}
	
	public double capacityAt(long epochSecond){
		
		Instant t = Instant.ofEpochSecond(epochSecond);
		DayOfWeek d = t.atZone(ZoneId.of("UTC")).getDayOfWeek();
		int idx = d.getValue() % 7;
		return mDailyCapacity[idx];
		
	}
	
	
}
