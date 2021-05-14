package pt.sotubo.planner.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.optaplanner.core.api.domain.entity.PlanningEntity;
import org.optaplanner.core.api.domain.variable.InverseRelationShadowVariable;


@PlanningEntity()
public class Resource implements Serializable{

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
	/*
	@InverseRelationShadowVariable(sourceVariableName = "resource")
	public Set<Operation> getOperationList(){ 
		return mOperations; 
	}
	public void setOperationList(Set<Operation> ops ) {
		mOperations = ops; 
	}
	*/
	public Resource(){
		mInstance = 0;
		mCapacity = 0;
		mDailyCapacity  = null;
		mCode  = null;
		mWorkcenter = null;
		mOperations = new HashSet<>();
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
	
	
	
}
