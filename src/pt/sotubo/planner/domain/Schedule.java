package com.joker.planner.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.SerializationUtils;
import org.optaplanner.core.api.domain.solution.PlanningEntityCollectionProperty;
import org.optaplanner.core.api.domain.solution.PlanningSolution;
import org.optaplanner.core.api.domain.solution.Solution;
import org.optaplanner.core.api.domain.solution.cloner.PlanningCloneable;
import org.optaplanner.core.api.domain.valuerange.ValueRangeProvider;
import org.optaplanner.core.api.score.buildin.bendablelong.BendableLongScore;

@PlanningSolution
public class Schedule implements Solution<BendableLongScore>, Serializable, PlanningCloneable<Schedule>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7161353027651922754L;
	private BendableLongScore score;

	@Override
	public BendableLongScore getScore() { return score; }
	@Override
	public void setScore(BendableLongScore sc) { score = sc; }
	

	private List<Resource> mResourceList;
	public void setResourceList(List<Resource> l) { mResourceList = l ;}
	public List<Resource> getResourceList() { return mResourceList; }

	private List<StockItem> mStockItemList;
	public void setStockItemList(List<StockItem> l) { mStockItemList = l ;}
	public List<StockItem> getStockItemList() { return mStockItemList; }
	
	private List<Operation> mOperationList;
	@PlanningEntityCollectionProperty
    public List<Operation> getOperationList() { return mOperationList; }
    public void setOperationList(List<Operation> operationList) { this.mOperationList = operationList; }

    
	private List<WorkOrder> mWorkOrderList;
	@PlanningEntityCollectionProperty
	public List<WorkOrder> getWorkOrderList() { return mWorkOrderList ;}
	public void setWorkOrderList(List<WorkOrder> l) { mWorkOrderList = l ;}
    
    
	private List<ExecutionGroup> mExecutionGroupList;
	public void setExecutionGroupList(List<ExecutionGroup> l) { mExecutionGroupList = l ;}
	public List<ExecutionGroup> getExecutionGroupList() { return mExecutionGroupList; }
    
	
	
	public Schedule(){
		mWorkOrderList = new ArrayList<WorkOrder>();
		mResourceList = new ArrayList<Resource>();
		mStockItemList = new ArrayList<StockItem>();
		mOperationList = new ArrayList<Operation>();
		mExecutionGroupList = new ArrayList<ExecutionGroup>();
	}
	
	
	@Override
	public Collection<? extends Object> getProblemFacts() {
		List<Object> facts = new ArrayList<Object>();
        //facts.addAll(mWorkOrderList);
        facts.addAll(mResourceList);
        facts.addAll(mStockItemList);
        facts.addAll(mExecutionGroupList);
        // Do not add the planning entity's (operationList) because that will be done automatically
        return facts;
	}
	

	@Override
	public Schedule planningClone() {
		
		//return this;
		return (Schedule)SerializationUtils.clone(this);
	}
	
	//@Override
	public Schedule planningClone_() {
		
		Schedule other = new Schedule();
		
		other.score = this.score == null ? null : this.score.multiply(1.0);
		
		for(WorkOrder wo : this.mWorkOrderList){
			other.mWorkOrderList.add(new WorkOrder(wo));
		}
		for(Operation op : this.mOperationList){
			other.mOperationList.add(new Operation(op));
		}
		
		//other.mResourceList.addAll(this.mResourceList);
		other.mStockItemList.addAll(this.mStockItemList);
		other.mExecutionGroupList.addAll(this.mExecutionGroupList);
		
		for(Resource res : this.mResourceList){
			other.mResourceList.add(new Resource(res));
		}
		/*
		for(StockItem it : this.mStockItemList){
			other.mStockItemList.add(new StockItem(it));
		}
		for(ExecutionGroup ex : this.mExecutionGroupList){
			other.mExecutionGroupList.add(new ExecutionGroup(ex));
		}
		*/
		
		int i;
		for(i = 0; i < this.mWorkOrderList.size(); i++){
			WorkOrder orig = this.mWorkOrderList.get(i);
			for(int j = 0; j < orig.getOperations().size(); j++ ){
				int idx = this.mOperationList.indexOf(orig.getOperations().get(j));
				other.mWorkOrderList.get(i).getOperations().set(j, other.mOperationList.get(idx));
			}
		}
		
		for(i = 0; i < this.mOperationList.size(); i++){
			Operation orig = this.mOperationList.get(i);
			Operation target = other.mOperationList.get(i);
			int idx = this.mWorkOrderList.indexOf(orig.getWorkOrder());
			target.setWorkOrder(other.mWorkOrderList.get(idx));
			
			idx = this.mResourceList.indexOf(orig.getResource());
			target.setResource(other.mResourceList.get(idx));
			
			for(int k = 0; k < orig.getResourceRange().size(); k++){
				idx = this.mResourceList.indexOf(orig.getResourceRange().get(k));	
				target.getResourceRange().set(k, other.mResourceList.get(idx));
			}
			
			
		}
		/*
		for(i = 0; i < this.mResourceList.size(); i++){
			Resource orig = this.mResourceList.get(i);
			Resource target = other.mResourceList.get(i);
			target.getOperationList().clear();
			for(Operation o : orig.getOperationList()){
				int idx = this.mOperationList.indexOf(o);
				assert(idx >= 0);
				target.getOperationList().add(other.mOperationList.get(idx));
			}
		}
		*/
		
		
		return other;
	}	
	
	
	@ValueRangeProvider(id = "existentStartDateRange")
    public Set<Long> getStartDateRange() {
    	
    	Set<Long> res = new HashSet<Long>();
    	
    	long now = System.currentTimeMillis()/1000;
    	res.add(now);
    	
    	now /= 24*3600;
    	now *= 24*3600;	//now at midnight
    	now += 12*3600;	//now at noon
    	for(long d = -5; d < 5; d++){
    		res.add(now + d*24*3600);	//5 days before/ 5 after now at noon
    	}
    	
    	List<Operation> ops = getOperationList();
    	for(Operation op : ops){
    		long t = op.getEndDate();
    		res.add(t);
    		res.add(t+60);
    		t = op.getStartDate();
    		res.add(t);
    		res.add(t-60);
    	}
    	return res;
    	
    }    
	
	
}
