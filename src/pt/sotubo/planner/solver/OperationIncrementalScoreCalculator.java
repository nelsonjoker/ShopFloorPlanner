package com.joker.planner.solver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.optaplanner.core.api.score.Score;
import org.optaplanner.core.api.score.buildin.bendablelong.BendableLongScore;
import org.optaplanner.core.api.score.buildin.hardsoft.HardSoftScore;
import org.optaplanner.core.impl.score.director.incremental.AbstractIncrementalScoreCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joker.planner.SortedArrayList;
import com.joker.planner.SortedList;
import com.joker.planner.domain.ExecutionGroup;
import com.joker.planner.domain.Operation;
import com.joker.planner.domain.Resource;
import com.joker.planner.domain.Schedule;
import com.joker.planner.domain.StockItem;
import com.joker.planner.domain.StockItemProductionTransaction;
import com.joker.planner.domain.StockItemTransaction;
import com.joker.planner.domain.WorkOrder;


public class OperationIncrementalScoreCalculator extends AbstractIncrementalScoreCalculator<Schedule>{

	private Map<Resource, ResourceCapacityTracker> resourceCapacityTrackerMap;
    private Map<StockItem, IStockItemCapacityTracker> stockTrackerMap;
    private Map<String, List<WorkOrder>> invoiceEndDateMap;
    //private Map<String, ExecutionGroup> invoiceExecutionGroup;	//maps VCRNUMORI to groups like INV_
    //private Map<Project, Long> projectStartDateMap;
    private Map<Resource, ExecutionGroupClusterTracker> executionGroupTrackerMap;
    private Map<Resource, Collection<Operation>> mResourceOperationMap;
    private List<WorkOrder> mDuplicateWOFilter;
    //private List<Operation> mOperationDbg;
    
    
    //private long maximumProjectEndDate;

    private long mScoreResourceCapacity;	//ensures resource capacity contraints
    private long mScoreStock;	//ensures stock sequencing constraint
    private long mScoreDeliveryDateAhead;	//minimizes delay for delivery time (optimizes for JIT)
    private long mScoreDeliveryDateBehind;
    private long mScoreMakespan;	//minimizes period span
    private HardSoftScore mScoreGrouping;	//maximizes grouping and provides nonvolatile replaning incentive 
    private long mOverlappedOperationScore;	//minimize the number of simultaneous operation so we get linear planning

    private Logger mLogger;
    //private Long mDbgOperationCount = null;
    //private Long mDbgWorkorderCount = null;
    
    private boolean mResetDone;
    
    public OperationIncrementalScoreCalculator(){
    	super();
    	mLogger = LoggerFactory.getLogger(OperationIncrementalScoreCalculator.class);
    	mDuplicateWOFilter = new LinkedList<WorkOrder>();
    	mResetDone = false;
    }
    
    public void resetWorkingSolution(Schedule schedule) {
    	
    	//if(mResetDone)
    	//	return;
    	
        List<Resource> resourceList = schedule.getResourceList();
        resourceCapacityTrackerMap = new HashMap<Resource, ResourceCapacityTracker>(resourceList.size());
        executionGroupTrackerMap = new HashMap<Resource, ExecutionGroupClusterTracker>(schedule.getWorkOrderList().size());
        mResourceOperationMap = new HashMap<Resource, Collection<Operation>>();
        for (Resource resource : resourceList) {
            //resourceCapacityTrackerMap.put(resource, resource.isRenewable() ? new RenewableResourceCapacityTracker(resource) : new NonrenewableResourceCapacityTracker(resource));
            resourceCapacityTrackerMap.put(resource, new RenewableResourceCapacityTracker(resource));
            executionGroupTrackerMap.put(resource, new ExecutionGroupClusterTracker());
            assert(!mResourceOperationMap.containsKey(resource));
            //List<Operation> ops = new LinkedList<Operation>();
            List<Operation> ops = new SortedList<Operation>(new Comparator<Operation>() {
				@Override
				public int compare(Operation o1, Operation o2) {
					return (int) (o1.getStartDate() - o2.getStartDate());
				}
			});
            /*
            TreeSet<Operation> ops = new TreeSet<Operation>(new Comparator<Operation>() {
				@Override
				public int compare(Operation o1, Operation o2) {
					return (int) (o1.getStartDate() - o2.getStartDate());
				}
			});
            */
            mResourceOperationMap.put(resource, ops);
        }
        
        
        List<StockItem> itemList = schedule.getStockItemList();
        stockTrackerMap = new HashMap<StockItem, IStockItemCapacityTracker>(itemList.size());
        for (StockItem i : itemList) {
        	assert(!stockTrackerMap.containsKey(i));
        	IStockItemCapacityTracker tr = i.getReocod() == 3 ? new StockItemCapacityTracker(i, i.getInitialAmount()) : new PurchaseStockItemCapacityTracker();
        	stockTrackerMap.put(i, tr);
        }
        
        //List<Project> projectList = schedule.getProjectList();
        invoiceEndDateMap = new HashMap<String, List<WorkOrder>>();
        //invoiceExecutionGroup = new HashMap<String, ExecutionGroup>();
        //projectStartDateMap = new HashMap<Project, Long>(projectList.size());
        
        //maximumProjectEndDate = 0;
        mScoreResourceCapacity = 0;
        mScoreStock = 0;
        mScoreDeliveryDateAhead = 0;
        mScoreDeliveryDateBehind = 0;
        mScoreMakespan = 0;
        mScoreGrouping = null;
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
        mDuplicateWOFilter.clear();
        for(Operation op : schedule.getOperationList()){
        	insert(op);
        	//insert(op.getWorkOrder());
        	
        	//integrity checkups
        	assert(op.getWorkOrder().getOperations().contains(op));
        	
        }
        /*
        mOperationDbg = schedule.getOperationList();
        assert(mDbgWorkorderCount == null || mDbgWorkorderCount == schedule.getWorkOrderList().size());
        assert(mDbgOperationCount == null || mDbgOperationCount == mOperationDbg.size());
        mDbgOperationCount = (long) mOperationDbg.size();
        mDbgWorkorderCount = (long) schedule.getWorkOrderList().size();
        */
        
        //assert(mOperationDbg.size() == 5000); //FIXME
        
        
        for(WorkOrder wo : schedule.getWorkOrderList()){
        	insert(wo);        	
        }
        
        mResetDone = true;
        mLogger.trace("resetWorkingSolution");
    }

    public void beforeEntityAdded(Object entity) {
    	//nothing
    	mResetDone = false;
    }

    public void afterEntityAdded(Object entity) {
    	mResetDone = false;
    	mLogger.debug("afterEntityAdded");
    	
    	if(entity instanceof Operation){
    		insert((Operation) entity);
    		//insert(((Operation) entity).getWorkOrder());
    	}else if(entity instanceof WorkOrder)
    		insert((WorkOrder) entity);
    	else
    		mLogger.error("Unhandled entity "+entity);
    }

    public void beforeVariableChanged(Object entity, String variableName) {
    	mResetDone = false;
    	//mLogger.debug("beforeVariableChanged "+variableName+" "+((Operation) entity).getWorkOrder());
    	//assert(((Operation) entity).getDuration() > 0);
    	switch(variableName){
    		case	"startDate":
    			retract((Operation) entity);
    			//retract(((Operation) entity).getWorkOrder());
    			break;
    		case	"resource":
    			retract((Operation) entity);
    			//retract(((Operation) entity).getWorkOrder());
    			break;
    		case	"firstStart":
    			retract((WorkOrder) entity);
    			break;
    		case	"operationList":
    			break;//hush    			
    		default:
    			mLogger.error("Unhandled variable "+variableName);
    			break;
    	}
        
    }

    public void afterVariableChanged(Object entity, String variableName) {
    	mResetDone = false;
    	//mLogger.debug("afterVariableChanged "+variableName+" "+((Operation) entity).getWorkOrder());
    	
	    switch(variableName){
			case	"startDate":
				insert((Operation) entity);
				//insert(((Operation) entity).getWorkOrder());
				break;
			case	"resource":
				insert((Operation) entity);
				//insert(((Operation) entity).getWorkOrder());
				break;
    		case	"firstStart":
    			insert((WorkOrder) entity);
    			break;
    		case	"operationList":
    			break;//hush
    		default:
    			mLogger.error("Unhandled variable "+variableName);
    			break;				
		}
    }

    public void beforeEntityRemoved(Object entity) {
    	mResetDone = false;
    	mLogger.debug("beforeEntityRemoved");
    	
    	if(entity instanceof Operation){
    		retract((Operation) entity);
    		//retract(((Operation) entity).getWorkOrder());
    	}else if(entity instanceof WorkOrder)
    		retract((WorkOrder) entity);
    	else
    		mLogger.error("Unhandled entity "+entity);
    }

    public void afterEntityRemoved(Object entity) {
        // Do nothing
    	mResetDone = false;
    }

    protected int overlapps(Collection<Operation> pool, Operation op){
    	int res = 0;
    	Long s = op.getStartDate();
    	Long e = op.getEndDate();
    	
    	for(Operation o : pool){
    		
    		if(s < o.getEndDate() && o.getStartDate() < e){
    			long ov = Math.min(o.getEndDate(), e) - Math.max(o.getStartDate(), s);
    			assert(ov > 0);
    			assert(ov <= o.getDuration());
    			assert(ov <= op.getDuration());
    			res+=ov;
    		}
    		
    	}
    	
    	return res;
    }
    private void insertz(Operation op) {
    	
    }
    private void insert(Operation op) {
    	
    	if(op.getStartDate() == null)
    		return;
    	
    	assert(op.getWorkOrder().getOperations().contains(op));
    	
    	ResourceCapacityTracker tracker = resourceCapacityTrackerMap.get(op.getResource());
        //hardScore -= tracker.getHardScore();
        tracker.insert(op.getResourceRequirement(), op);
        //hardScore += tracker.getHardScore();
    	
        ExecutionGroupClusterTracker clTracker = executionGroupTrackerMap.get(op.getResource());
        clTracker.insert(op.getStartDate(), op.getWorkOrder());
        
        
        Collection<Operation> pool = mResourceOperationMap.get(op.getResource());
        mOverlappedOperationScore -= overlapps(pool, op); 
        assert(!pool.contains(op));
        pool.add(op);
        
        //mLogger.debug("mOverlappedOperationScore("+op.getResource()+"/"+op.getResource().hashCode()+") insert = "+mOverlappedOperationScore);
    }
    
    private void retractz(Operation op) {
    	
    }
    private void retract(Operation op) {
    	
    	if(op.getStartDate() == null)
    		return;
    	
    	assert(op.getWorkOrder().getOperations().contains(op));
    	
    	ResourceCapacityTracker tracker = resourceCapacityTrackerMap.get(op.getResource());
        //hardScore -= tracker.getHardScore();
        tracker.retract(op.getResourceRequirement(), op);
        //hardScore += tracker.getHardScore();
    	
        ExecutionGroupClusterTracker clTracker = executionGroupTrackerMap.get(op.getResource());
        clTracker.retract(op.getStartDate(), op.getWorkOrder());
        
        Collection<Operation> pool = mResourceOperationMap.get(op.getResource());
        assert(pool.contains(op));
        pool.remove(op);
        assert(!pool.contains(op));
        mOverlappedOperationScore += overlapps(pool, op);
        
        //mLogger.debug("mOverlappedOperationScore("+op.getResource()+"/"+op.getResource().hashCode()+") retract = "+mOverlappedOperationScore);
        
    }
    private void insertz(WorkOrder wo) {
    	
    }
    private void insert(WorkOrder wo) {
    	
    	if(wo.getFirstStart() == null)
    		return;
    	
    	/*
    	//process only first time we see this wo
        if(mDuplicateWOFilter.contains(wo)){
        	assert(wo.getOperations().size() > 0);
        	mDuplicateWOFilter.add(wo);
        	return;
        }else{
        	mDuplicateWOFilter.add(wo);
        }
    	*/
        for (StockItemTransaction stockRequirement : wo.getRequiredTransaction()) {
            IStockItemCapacityTracker stTracker = stockTrackerMap.get(stockRequirement.getItem());
            stTracker.insertRequirement(stockRequirement, wo);
        }
        for (StockItemProductionTransaction stockProduction : wo.getProducedTransactionList()) {
            IStockItemCapacityTracker stTracker = stockTrackerMap.get(stockProduction.getItem());
            stTracker.insertProduction(stockProduction, wo);

	        List<WorkOrder> invoiceList = invoiceEndDateMap.get(stockProduction.getVCRNUMORI());
	        if(invoiceList == null){
	        	//invoiceList = new LinkedList<>();
	        	invoiceList = new SortedArrayList<>(new Comparator<WorkOrder>() {
					@Override
					public int compare(WorkOrder o1, WorkOrder o2) {
						return o1.getLastEnd().compareTo(o2.getLastEnd());
					}
				});
	        	invoiceEndDateMap.put(stockProduction.getVCRNUMORI(), invoiceList);
	        }
	        invoiceList.add(wo);
	        
            mScoreMakespan -= invoiceList.get(invoiceList.size() - 1).getLastEnd() - invoiceList.get(0).getFirstStart();
            
            /*
	        TreeSet<WorkOrder> invoiceList = invoiceEndDateMap.get(stockProduction.getVCRNUMORI());
	        if(invoiceList == null){
	        	invoiceList = new TreeSet<WorkOrder>(new Comparator<WorkOrder>() {
					@Override
					public int compare(WorkOrder o1, WorkOrder o2) {
						return o1.getLastEnd().compareTo(o2.getLastEnd());
					}
				});
	        	invoiceEndDateMap.put(stockProduction.getVCRNUMORI(), invoiceList);
	        }
	        assert(!invoiceList.contains(wo));
	        invoiceList.add(wo);
	        */
        }
        /*
        Long computedLast = null;
        for(Operation o : wo.getOperations()){
        	if(computedLast == null || o.getEndDate() > computedLast){
        		computedLast = o.getEndDate();
        	}
        }
        assert(computedLast == null || computedLast.equals( wo.getLastEnd()) );
        */
        if(!wo.getOperations().isEmpty()){
	        Long t = wo.getExecutionGroupForcedEnd();
	        if(t != null){
	        	long dly = wo.getLastEnd() - t;
	        	if(dly > 0)
	    			mScoreDeliveryDateBehind -= dly;
	        }
	        
	        t = wo.getExecutionGroupForcedStart();
	        if(t != null){
	        	long dly = wo.getFirstStart() - t; 
	    		if(dly < 0)
	    			mScoreDeliveryDateAhead += dly; 
	        }
        }
        
    }

    
    private void retractz(WorkOrder wo) {
    	
    }
    private void retract(WorkOrder wo) {
    	
    	if(wo.getFirstStart() == null)
    		return;

    	
    	/*
    	assert(mDuplicateWOFilter.contains(wo));
    	
    	mDuplicateWOFilter.remove(wo);
    	if(mDuplicateWOFilter.contains(wo))
    		return;	//only the last one is processed
        */
    	
        for (StockItemTransaction stockRequirement : wo.getRequiredTransaction()) {
            IStockItemCapacityTracker stTracker = stockTrackerMap.get(stockRequirement.getItem());
            stTracker.retractRequirement(stockRequirement, wo);
        }
        for (StockItemProductionTransaction stockProduction : wo.getProducedTransactionList()) {
            IStockItemCapacityTracker stTracker = stockTrackerMap.get(stockProduction.getItem());
            stTracker.retractProduction(stockProduction, wo);
        
            assert(invoiceEndDateMap.containsKey(stockProduction.getVCRNUMORI()));
	        List<WorkOrder> invoiceList = invoiceEndDateMap.get(stockProduction.getVCRNUMORI());
	        assert(invoiceList.contains(wo));
	        mScoreMakespan += invoiceList.get(invoiceList.size() - 1).getLastEnd() - invoiceList.get(0).getFirstStart();
	        invoiceList.remove(wo);
	    }
        /*
        Long computedLast = null;
        for(Operation o : wo.getOperations()){
        	if(computedLast == null || o.getEndDate() > computedLast){
        		computedLast = o.getEndDate();
        	}
        }
        assert(computedLast == null || computedLast.equals( wo.getLastEnd()) );
        */
        
        if(!wo.getOperations().isEmpty()){
        
	        Long t = wo.getExecutionGroupForcedEnd();
	        if(t != null){
	        	long dly = wo.getLastEnd() - t;
	        	if(dly > 0)
	    			mScoreDeliveryDateBehind += dly;
	        }
	        
	        t = wo.getExecutionGroupForcedStart();
	        if(t != null){
	        	long dly = wo.getFirstStart() - t; 
	    		if(dly < 0)
	    			mScoreDeliveryDateAhead -= dly; 
	        }
        }
        
        
    }
    
    /*
    private void updateMaximumProjectEndDate() {
        long maximum = 0;
        for (Long endDate : projectEndDateMap.values()) {
            if (endDate > maximum) {
                maximum = endDate;
            }
        }
        maximumProjectEndDate = maximum;
    }
    */

    public Score calculateScore() {
    	//mLogger.debug("calc...");
    	
    	mScoreResourceCapacity = 0;
    	for(ResourceCapacityTracker t : resourceCapacityTrackerMap.values()){
    		mScoreResourceCapacity += t.getHardScore();
    	}
    	//mScoreResourceCapacity += mOverlappedOperationScore;
    	
    	mScoreStock = 0;
    	
    	for(IStockItemCapacityTracker tr : stockTrackerMap.values()){
    		mScoreStock += tr.getHardScore();
    	}
    	
    	
    	
    	//mScoreDeliveryDateAhead = 0;
    	//mScoreDeliveryDateBehind = 0;
    	mScoreMakespan = 0;
    	//double score0 = 0;
    	//soft1Score = 0;
    	
    	//mScoreStock = 0;
    	
    	Set<String> keys = invoiceEndDateMap.keySet();
    	for(String i : keys){
    		List<WorkOrder> a = invoiceEndDateMap.get(i);
    		if(a.size() == 0){
    			continue;
    		}
    	
    		
    		long dueDate = 0;
    		long lastDate = 0;
    		long firstDate = Long.MAX_VALUE;
    		for(WorkOrder wo  : a){
    			/*
    			WorkOrder next = wo.getNextOrder();
    			if(next != null && next.getFirstStart() < wo.getLastEnd()){
    				mScoreStock -=   wo.getLastEnd() - next.getFirstStart();
    			}
    			*/
    			if(dueDate == 0){
	    			for(ExecutionGroup g : wo.getExecutionGroups()){
	    				String code = g.getCode();
	            		if(code.startsWith("INV_") && g.getEndTime() != null){
	            			dueDate = g.getEndTime();
	            			break;
	            		}
	    			}
    			}
    			
    			if(wo.getLastEnd() > lastDate){
    				lastDate = wo.getLastEnd();
    			}
    			if(wo.getFirstStart() < firstDate){
    				firstDate = wo.getFirstStart();
    			}
    		}
    		/*
    		if(dueDate > 0 && lastDate > 0){
    			assert(dueDate > 2*24*3600);
    			long max = dueDate - 24*3600;
    			long min = max - 24*3600;
    			long delay = 0;
    			if(lastDate > max){
    				delay = lastDate - max; 
    				mScoreDeliveryDateBehind -= delay;
    			}else if(lastDate < min){
    				delay = (min - lastDate);
    				mScoreDeliveryDateAhead -= delay;
    			}
    			
    			//mScoreDeliveryDate -= delay*delay; //squaring the delay makes us act on further away instances
    		}
    		*/
    		if(lastDate > 0 && firstDate < Long.MAX_VALUE)
    			mScoreMakespan -= lastDate - firstDate;
    		
    		
    	}
    	
    	
    	
    	long mIdleResourceScore = 0;    	
    	
    	List<Long> lagsMedian = new SortedArrayList<Long>(new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				return o1.compareTo(o2);
			}
		});
    	//mScoreMakespan = 0;
    	for(Resource r : mResourceOperationMap.keySet()){
    		Collection<Operation> sorted = mResourceOperationMap.get(r);
    		if(sorted.size() < 3)
    			continue;
    		Iterator<Operation> it = sorted.iterator();
    		
    		long lags = 0;
    		long ileft = 0;
    		long iright = 0;
    		
    		Operation left = it.next();
    		Operation op = it.next();
    		while(it.hasNext()){
    			Operation right = it.next();
    			
    			ileft = Math.max(op.getStartDate() - left.getEndDate(), 0);
    			iright = Math.max(right.getStartDate() - op.getEndDate(), 0);
    			
    			lags += Math.min(ileft, iright);
    			
    			left = op;
    			op = right;
    		}
    		/*
    		ArrayList<Operation> ops = new ArrayList<>(sorted);
    		long lastEnd = ops.get(ops.size()-1).getEndDate();
    		long lags = 0;
    		for(int i = ops.size() - 2; i >= 0; i--){
    			long l = lastEnd - ops.get(i).getEndDate();
    			lags += l;
    			//lagsMedian.add(l);
    		}
    		*/
    		/*
    		
    		Iterator<Operation> it = sorted.iterator();
    		Operation prev = it.next();
    		Operation next = null;
    		while(it.hasNext()){
    			assert(r.equals(prev.getResource()));
    			next = it.next();
    			assert(prev.getStartDate() <= next.getStartDate());
    			lags += (next.getStartDate() - prev.getEndDate());    			
    			prev = next;
    		}
    		*/
    		mIdleResourceScore -= lags;
    		//if(lags != 0)
    		//	mIdleResourceScore--;
    	}
    	
    	//mIdleResourceScore = lagsMedian.size() > 2 ? -1*lagsMedian.get(lagsMedian.size() / 2) : 0;
    	
    	
    	mScoreGrouping = HardSoftScore.valueOf(0, 0);
    	
    	for(ExecutionGroupClusterTracker tr : executionGroupTrackerMap.values()){
    		mScoreGrouping = mScoreGrouping.add(tr.getScore());
    	}    	
    	
        return BendableLongScore.valueOf(new long[] { mScoreDeliveryDateBehind, mScoreStock,  mScoreResourceCapacity }, new long[] { mOverlappedOperationScore, mScoreGrouping.getHardScore(), mScoreGrouping.getSoftScore(), mScoreDeliveryDateAhead, mScoreMakespan, mIdleResourceScore});
        //return BendableLongScore.valueOf(new long[] { mScoreDeliveryDateBehind + mScoreStock + 1000*mScoreResourceCapacity + mScoreGrouping.getHardScore() + mOverlappedOperationScore, 0, 0 }, new long[] {mScoreDeliveryDateBehind, mScoreStock,  mScoreResourceCapacity , mScoreGrouping.getHardScore(), mOverlappedOperationScore, 0});
    }

}
