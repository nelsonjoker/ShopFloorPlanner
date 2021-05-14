package pt.sotubo.planner.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.SerializationUtils;

import pt.sotubo.planner.solver.Score;

public class Schedule implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7161353027651922754L;
	private Score score;

	public Score getScore() { return score; }
	public void setScore(Score sc) { score = sc; }
	

	private List<Resource> mResourceList;
	public List<Resource> getResourceList() { return mResourceList; }
	public void setResourceList(List<Resource> l) { mResourceList = l ;}

	private List<StockItem> mStockItemList;
	public List<StockItem> getStockItemList() { return mStockItemList; }
	public void setStockItemList(List<StockItem> l) { mStockItemList = l ;}
	
	private List<Operation> mOperationList;
    public List<Operation> getOperationList() { return mOperationList; }
    public void setOperationList(List<Operation> operationList) { this.mOperationList = operationList; }

    
	private List<WorkOrder> mWorkOrderList;
	public List<WorkOrder> getWorkOrderList() { return mWorkOrderList ;}
	public void setWorkOrderList(List<WorkOrder> l) { mWorkOrderList = l ;}
    
    
	private List<ExecutionGroup> mExecutionGroupList;
	public List<ExecutionGroup> getExecutionGroupList() { return mExecutionGroupList; }
	public void setExecutionGroupList(List<ExecutionGroup> l) { mExecutionGroupList = l ;}
    
	
	
	public Schedule(){
		mWorkOrderList = new ArrayList<WorkOrder>();
		mResourceList = new ArrayList<Resource>();
		mStockItemList = new ArrayList<StockItem>();
		mOperationList = new ArrayList<Operation>();
		mExecutionGroupList = new ArrayList<ExecutionGroup>();
	}
	

	//@Override
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
	
	
	
}




