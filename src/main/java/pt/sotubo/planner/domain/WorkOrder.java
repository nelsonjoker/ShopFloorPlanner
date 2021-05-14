package pt.sotubo.planner.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class WorkOrder implements Serializable{

	private List<StockItemProductionTransaction> mProducedTransaction;
	private List<StockItemTransaction> mRequiredTransaction;
	private List<ExecutionGroup> mExecutionGroups;
	private List<Operation> mOperations;
	private Long executionGroupForcedEnd;
	private Long executionGroupForcedStart;
	private String mMFGNUM;
	public String getMFGNUM() { return mMFGNUM; }
	public void setMFGNUM(String c) { mMFGNUM = c; }
	private Long mLastEnd;
	private Long mFirstStart;
	private WorkOrder mNextWorkOrder;
	
	public WorkOrder(){
		this.mProducedTransaction = null;
		this.mRequiredTransaction = null;
		this.mExecutionGroups = null;
		this.mOperations = null;
		this.executionGroupForcedEnd = null;
		this.executionGroupForcedStart = null;
		this.mMFGNUM = null;
		this.mLastEnd = 0L;
		this.mFirstStart = 0L;
		mNextWorkOrder = null;
	}
	
	public WorkOrder(WorkOrder wo){
		this.mProducedTransaction = new ArrayList<StockItemProductionTransaction>(wo.mProducedTransaction);
		this.mRequiredTransaction = new ArrayList<StockItemTransaction>(wo.mRequiredTransaction);
		this.mExecutionGroups = new ArrayList<ExecutionGroup>(wo.mExecutionGroups);
		this.mOperations = new ArrayList<Operation>(wo.mOperations);
		this.executionGroupForcedEnd = wo.executionGroupForcedEnd == null ? null : new Long(wo.executionGroupForcedEnd);
		this.executionGroupForcedStart = wo.executionGroupForcedStart == null ? null : new Long(wo.executionGroupForcedStart);
		this.mMFGNUM = wo.mMFGNUM;
		this.mLastEnd = new Long(wo.mLastEnd);
		this.mFirstStart = new Long(wo.mFirstStart);
		this.mNextWorkOrder = wo.mNextWorkOrder;
	}
	
	public List<StockItemProductionTransaction> getProducedTransactionList() {
		return mProducedTransaction;
	}
	public void setProducedTransaction(List<StockItemProductionTransaction> producedTransaction) {
		mProducedTransaction = producedTransaction;
	}
	public List<StockItemTransaction> getRequiredTransaction() {
		return mRequiredTransaction;
	}
	public void setRequiredTransaction(List<StockItemTransaction> requiredTransaction) {
		mRequiredTransaction = requiredTransaction;
	}
	public List<ExecutionGroup> getExecutionGroups() {
		return mExecutionGroups;
	}
	public void setExecutionGroups(List<ExecutionGroup> executionGroups) {
		mExecutionGroups = executionGroups;
		Long min = Long.MAX_VALUE;
    	Long max = Long.MIN_VALUE;
    	Long invEnd = null;
    	if(mExecutionGroups != null){
    		for(ExecutionGroup g : mExecutionGroups){
    			if(g.getStartTime() != null && g.getStartTime() < min)
    				min = g.getStartTime();
    			if(g.getEndTime() != null && g.getEndTime() > max)
    				max = g.getEndTime();
    			if(g.getCode().startsWith("INV_") && g.getEndTime() != null)
    				invEnd = g.getEndTime();
    		}
    	}
    	
    	if(invEnd != null)
    		max = invEnd;
    	
    	if(min < Long.MAX_VALUE){
    		executionGroupForcedStart = min;
    	}else{
    		executionGroupForcedStart = null;
    	}
    	if(max > Long.MIN_VALUE){
    		executionGroupForcedEnd = max;
    	}else{
    		executionGroupForcedEnd = null;
    	}
	}
     

    
    public Long getExecutionGroupForcedEnd(){
    	return executionGroupForcedEnd;
    }
    public Long getExecutionGroupForcedStart(){
    	return executionGroupForcedStart;
    }
	
	public List<Operation> getOperations() {
		return mOperations;
	}
	public void setOperations(List<Operation> operations) {
		mOperations = operations;
	}
	
	//public WorkOrder getNextOrder(){ return mNextWorkOrder; }
	//public void setNextOrder(WorkOrder wo) { mNextWorkOrder = wo; }
	


	
	//OperationDateUpdatingVariableListener is called after the score calculator
	//so we would have inserted a start date on cluster tracker before it was updated
	public Long getFirstStart() { return mFirstStart; }
	public void setFirstStart(Long st) { 
		mFirstStart = st; 
	}

	//firstStart actually updates both
	//@CustomShadowVariable(variableListenerRef = @PlanningVariableReference(variableName = "firstStart"))
	public Long getLastEnd() { return mLastEnd; }
	public void setLastEnd(long lastEnd) { 
		mLastEnd = lastEnd; 
	}
	
	/*
	public long getFirstStart() {
		Long t = null;
		for(Operation o : mOperations){
			if( t == null || o.getStartDate() < t)
				t = o.getStartDate();
		}
		return t == null ? mFirstStart.longValue() : t.longValue();
	}
	
	public long getLastEnd() { 
		Long t = null;
		for(Operation o : mOperations){
			if( t == null || o.getEndDate() > t)
				t = o.getEndDate();
		}
		return t == null ? mLastEnd.longValue() : t.longValue();
	}
	*/
	public boolean isItemReferenced(StockItem it){
		for(StockItemProductionTransaction t : mProducedTransaction){
			if(t.getItem().equals(it))
				return true;
		}
		for(StockItemTransaction t : mRequiredTransaction){
			if(t.getItem().equals(it))
				return true;
		}
		return false;
	}
	
	@Override
	public boolean equals(Object obj) {
		WorkOrder other = (WorkOrder) obj;
		return other.mMFGNUM.equals(this.mMFGNUM);
	}
	
	@Override
	public String toString() {
		return mMFGNUM;
	}
}
