package com.joker.planner.solver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.optaplanner.core.api.score.Score;
import org.optaplanner.core.api.score.buildin.bendable.BendableScore;
import org.optaplanner.core.impl.score.director.easy.EasyScoreCalculator;
import com.joker.planner.domain.ExecutionGroup;
import com.joker.planner.domain.Operation;
import com.joker.planner.domain.Resource;
import com.joker.planner.domain.Schedule;
import com.joker.planner.domain.StockItem;
import com.joker.planner.domain.StockItemProductionTransaction;
import com.joker.planner.domain.StockItemTransaction;
import com.joker.planner.domain.WorkOrder;

/**
 * couldn't get the incremental calculator to work so implemented an easy one
 * @author Nelson
 *
 */
public class OperationEasyScoreCalculator implements EasyScoreCalculator<Schedule> {

	private Map<Resource, ResourceCapacityTracker> resourceCapacityTrackerMap;
    private Map<StockItem, StockItemCapacityTracker> stockTrackerMap;
    private Map<String, List<WorkOrder>> invoiceEndDateMap;
    private Map<Resource, ExecutionGroupClusterTracker> executionGroupTrackerMap;
    private Map<Resource, List<Operation>> mResourceOperationMap;
    
    private int hardScore;	//ensures resource capacity contraints
    private int hard1Score;	//ensures stock sequencing constraint
    private int soft0Score;	//minimizes delay for delivery time (optimizes for JIT)
    private int soft1Score;	//minimizes period span
    private int soft2Score;	//maximizes grouping and provides nonvolatile replaning incentive 
    private int mOverlappedOperationScore;	//minimize the number of simultaneous operation so we get linear planning

	
	private void resetWorkingSolution(Schedule schedule) {
        List<Resource> resourceList = schedule.getResourceList();
        resourceCapacityTrackerMap = new HashMap<Resource, ResourceCapacityTracker>(resourceList.size());
        executionGroupTrackerMap = new HashMap<Resource, ExecutionGroupClusterTracker>(schedule.getWorkOrderList().size());
        mResourceOperationMap = new HashMap<Resource, List<Operation>>();
        for (Resource resource : resourceList) {
            //resourceCapacityTrackerMap.put(resource, resource.isRenewable() ? new RenewableResourceCapacityTracker(resource) : new NonrenewableResourceCapacityTracker(resource));
            resourceCapacityTrackerMap.put(resource, new RenewableResourceCapacityTracker(resource));
            executionGroupTrackerMap.put(resource, new ExecutionGroupClusterTracker());
            assert(!mResourceOperationMap.containsKey(resource));
            mResourceOperationMap.put(resource, new ArrayList<Operation>());
        }
        
        
        List<StockItem> itemList = schedule.getStockItemList();
        stockTrackerMap = new HashMap<StockItem, StockItemCapacityTracker>(itemList.size());
        for (StockItem i : itemList) {
        	assert(!stockTrackerMap.containsKey(i));
        	stockTrackerMap.put(i, new StockItemCapacityTracker(i, i.getInitialAmount()));
        }
        
        //List<Project> projectList = schedule.getProjectList();
        invoiceEndDateMap = new HashMap<String, List<WorkOrder>>();
        //invoiceExecutionGroup = new HashMap<String, ExecutionGroup>();
        //projectStartDateMap = new HashMap<Project, Long>(projectList.size());
        
        //maximumProjectEndDate = 0;
        hardScore = 0;
        hard1Score = 0;
        soft0Score = 0;
        soft1Score = 0;
        soft2Score = 0;
        mOverlappedOperationScore = 0;
        /*
        long minimumReleaseDate = Long.MAX_VALUE;
        for (Project p: projectList) {
            minimumReleaseDate = Math.min(p.getReleaseDate(), minimumReleaseDate);
        }
        //soft1Score += minimumReleaseDate;
        for (Allocation allocation : schedule.getAllocationList()) {
            insert(allocation);
        }
        */
        for(Operation op : schedule.getOperationList()){
        	insert(op);
        	//integrity checkups
        	assert(op.getWorkOrder().getOperations().contains(op));
        }
        
        for(WorkOrder wo : schedule.getWorkOrderList()){
        	insert(wo);
        }
        
        //mLogger.debug("resetWorkingSolution");
    }
	
	
	protected int overlapps(List<Operation> pool, Operation op){
    	int res = 0;
    	Long s = op.getStartDate();
    	Long e = op.getEndDate();
    	
    	for(Operation o : pool){
    		
    		if(s <= o.getEndDate() && o.getStartDate() <= e)
    			res++;
    		
    	}
    	
    	return res;
    }
   
    private void insert(Operation op) {
    	
    	assert(op.getWorkOrder().getOperations().contains(op));
    	
    	ResourceCapacityTracker tracker = resourceCapacityTrackerMap.get(op.getResource());
        hardScore -= tracker.getHardScore();
        tracker.insert(op.getResourceRequirement(), op);
        hardScore += tracker.getHardScore();
    	
        ExecutionGroupClusterTracker clTracker = executionGroupTrackerMap.get(op.getResource());
        clTracker.insert(op.getStartDate(), op.getWorkOrder());
        
        
        List<Operation> pool = mResourceOperationMap.get(op.getResource());
        mOverlappedOperationScore -= overlapps(pool, op); 
        //if(overlapps(pool, op))
        //	mOverlappedOperationScore--;
        assert(!pool.contains(op));
        pool.add(op);
        
        //mLogger.debug("mOverlappedOperationScore("+op.getResource()+"/"+op.getResource().hashCode()+") insert = "+mOverlappedOperationScore);
    }
   
    
    private void insert(WorkOrder wo) {
    	
        for (StockItemTransaction stockRequirement : wo.getRequiredTransaction()) {
            IStockItemCapacityTracker stTracker = stockTrackerMap.get(stockRequirement.getItem());
            stTracker.insertRequirement(stockRequirement, wo);
        }
        for (StockItemProductionTransaction stockProduction : wo.getProducedTransactionList()) {
            IStockItemCapacityTracker stTracker = stockTrackerMap.get(stockProduction.getItem());
            stTracker.insertProduction(stockProduction, wo);

	        List<WorkOrder> invoiceList = invoiceEndDateMap.get(stockProduction.getVCRNUMORI());
	        if(invoiceList == null){
	        	invoiceList = new LinkedList<>();
	        	invoiceEndDateMap.put(stockProduction.getVCRNUMORI(), invoiceList);
	        }
	        invoiceList.add(wo);
	        
        }
        
    }

    
	@Override
	public Score<BendableScore> calculateScore(Schedule schedule) {
		
		resetWorkingSolution(schedule);

		hard1Score = 0;
    	
    	for(IStockItemCapacityTracker tr : stockTrackerMap.values()){
    		hard1Score += tr.getHardScore();
    	}
    	
    	hard1Score += mOverlappedOperationScore;
    	
    	soft0Score = 0;
    	double score0 = 0;
    	soft1Score = 0;
    	
    	Set<String> keys = invoiceEndDateMap.keySet();
    	for(String i : keys){
    		List<WorkOrder> a = invoiceEndDateMap.get(i);
    		a.sort(new Comparator<WorkOrder>() {
				@Override
				public int compare(WorkOrder o1, WorkOrder o2) {
					return (int) (o1.getLastEnd() - o2.getLastEnd());
				}
			});
    		WorkOrder last = a.get(a.size() - 1);
    		WorkOrder first = a.get(0);
    		assert(last.getLastEnd() >= first.getLastEnd());
    		Long dueDate = null;
        	for(ExecutionGroup g : last.getExecutionGroups()){
        		String code = g.getCode();
        		if(code.startsWith("INV_") && g.getEndTime() != null){
        			dueDate = g.getEndTime();
        			break;
        		}
        	}
    		
    		if(dueDate != null)
    		{
    			score0 += ((dueDate - last.getLastEnd())/(3600.0*24));
    		}
    		soft1Score -= (last.getLastEnd() - first.getFirstStart());
    	}
    	
    	
    	
    	score0 = -score0*score0 + 2*score0;
    	soft0Score = (int)(score0*3600*24);
    	
    	
    	soft2Score = 0;
    	for(ExecutionGroupClusterTracker tr : executionGroupTrackerMap.values()){
 //   		soft2Score += tr.getScore();
    	}
		
		
		return BendableScore.valueOf(new int[] {hardScore, hard1Score}, new int[] {soft0Score, soft1Score, soft2Score});
    	//return BendableScore.valueOf(new int[] {hard1Score}, new int[] {});
	}

}
