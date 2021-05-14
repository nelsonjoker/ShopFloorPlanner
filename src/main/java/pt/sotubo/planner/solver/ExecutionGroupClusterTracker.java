package pt.sotubo.planner.solver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import pt.sotubo.planner.domain.ExecutionGroup;
import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;

/**
 * Measures the similarity between adjacent allocations
 * for a given resource...
 * This measure is the number of different groups between allocation...
 * if the component changes the weight on the score is worst 100x than any other group...
 * 
 * Also penalizes changes on the most upcoming elements
 * and adds a little bit of weight for score forward
 * @author Nelson
 */
public class ExecutionGroupClusterTracker {

    private TreeMap<Long, List<WorkOrder>> allocationDateMap;
    //private int mScore;
    /**
     * this gets worst when planning messes with first allocations...
     * reset after getScore is called
     */
    //private int mVolatileScore;
    private Score mScore;
//    private List<WorkOrder> mDuplicateWorkOrderFilter;

    public ExecutionGroupClusterTracker(){
    	allocationDateMap = new TreeMap<Long, List<WorkOrder>>();
    	//mScore = 0;
    	//mVolatileScore = 0;
    	mScore = Score.valueOf(0, 0); 
//    	mDuplicateWorkOrderFilter = new ArrayList<WorkOrder>();
    }
    
    /**
     * insert a new workorder at time startTime
     * to insert we need to interrupt the chain of WOs
     * we may be adding a new t or apending to an existent one...
     * so first we need to break the chain...
     * therefore remove the score between the ceiling of t and previous
     * then insert wo
     * add the score 
     * 
     * @param startTime
     * @param wo
     */
    public void insert(long startTime, WorkOrder wo) {
    	
    	/*
    	 * to insert the work order we need to remove score
    	 * of previous pair
    	 * insert the new wo 
    	 * add the score again
    	 */
    	
    	
    	//ExecutionMode executionMode = allocation.getExecutionMode();
    	
    	//long k = wo.getFirstStart();
        //System.out.println("inserting "+wo+" at "+k);
    	Entry<Long, List<WorkOrder>> k = allocationDateMap.containsKey(startTime) ? allocationDateMap.ceilingEntry(startTime) : null;
    	Entry<Long, List<WorkOrder>> nextEntry = allocationDateMap.higherEntry(startTime);
    	Entry<Long, List<WorkOrder>> afterNextEntry = nextEntry == null ? null : allocationDateMap.higherEntry(nextEntry.getKey());
    	Entry<Long, List<WorkOrder>> prevEntry = allocationDateMap.lowerEntry(startTime);
    	Entry<Long, List<WorkOrder>> beforePrevEntry = prevEntry == null ? null : allocationDateMap.lowerEntry(prevEntry.getKey());
    	
    	if(k != null){
    		/* key exists...
    		 * remove score btw k and prev, btw k and beforePrev, btw next and k, btw after next and k
    		 * add the wo 
    		 * add score  btw k and prev, betw k and beforePrev, btw next and k, btw afterNext and k
    		 */
    		if(beforePrevEntry != null)
    			mScore = mScore.subtract(score(k.getValue(), beforePrevEntry.getValue(), k.getKey() - beforePrevEntry.getKey()));
    		if(prevEntry != null)
    			mScore = mScore.subtract(score(k.getValue(), prevEntry.getValue(), k.getKey() - prevEntry.getKey()));
    		if(nextEntry != null)
    			mScore = mScore.subtract(score(nextEntry.getValue(), k.getValue(), nextEntry.getKey() - k.getKey()));
    		if(afterNextEntry != null)
    			mScore = mScore.subtract(score(afterNextEntry.getValue(), k.getValue(), afterNextEntry.getKey() - k.getKey()));
    		
    		k.getValue().add(wo);
    		
    		
    	}else{
    		/* need to insert new entry
    		 * remove score btw next and prev , btw next and beforePrev, afterNext and previous
    		 * insert the wo in new position k
    		 * add score  btw k and prev, btw k and beforePrev, btw next and k, and btw next and prev, afterNext and k
    		 */
    		if(nextEntry != null && beforePrevEntry != null)
    			mScore = mScore.subtract(score(nextEntry.getValue(), beforePrevEntry.getValue(), nextEntry.getKey() - beforePrevEntry.getKey()));
    		if(nextEntry != null && prevEntry != null)
    			mScore = mScore.subtract(score(nextEntry.getValue(), prevEntry.getValue(), nextEntry.getKey() - prevEntry.getKey()));
    		if(afterNextEntry != null && prevEntry != null)
    			mScore = mScore.subtract(score(afterNextEntry.getValue(), prevEntry.getValue(), afterNextEntry.getKey() - prevEntry.getKey()));
    		
    		List<WorkOrder> ex = new LinkedList<WorkOrder>();
    		ex.add(wo);
    		allocationDateMap.put(startTime, ex);
    		
    		k = allocationDateMap.ceilingEntry(startTime);
    		
    		
    		if(prevEntry != null && nextEntry != null)
    			mScore = mScore.add(score(nextEntry.getValue(), prevEntry.getValue(), nextEntry.getKey() - prevEntry.getKey()));
    		
    		//System.out.println("adding key "+ startTime);
    		
    	}
		if(beforePrevEntry != null)
			mScore = mScore.add(score(k.getValue(), beforePrevEntry.getValue(), k.getKey() - beforePrevEntry.getKey()));
		if(prevEntry != null)
			mScore = mScore.add(score(k.getValue(), prevEntry.getValue(), k.getKey() - prevEntry.getKey()));
		if(nextEntry != null)
			mScore = mScore.add(score(nextEntry.getValue(), k.getValue(), nextEntry.getKey() - k.getKey()));
		if(afterNextEntry != null)
			mScore = mScore.add(score(afterNextEntry.getValue(), k.getValue(), afterNextEntry.getKey() - k.getKey()));
    	
    	
    	/*
        if(nextEntry != null && prevEntry != null){
        	mScore += score(nextEntry.getValue(), prevEntry.getValue());
        }
    	
    	
    	List<WorkOrder> ex = allocationDateMap.get(startTime);
    	if(ex == null){
    		ex = new LinkedList<WorkOrder>();
    		allocationDateMap.put(startTime, ex);
    		mScore -= score(nextEntry.getValue(), ex);
    	}
    	ex.add(wo);
        
        if(prevEntry != null){
        	mScore -= score(ex, prevEntry.getValue());
        }
        */
       
		
    }
    


    public void retract(long startTime, WorkOrder wo) {
    	/**
    	 * to retract we need to remove the score on current set
    	 * then remove the wo
    	 * recalculate the score on the same interval
    	 */
    	
    	
    	//ExecutionMode executionMode = allocation.getExecutionMode();
    	
    	
    	//long k = wo.getFirstStart();        
    	//System.out.println("retract "+wo+" at "+k);
    	assert(allocationDateMap.containsKey(startTime));
    	
    	Entry<Long, List<WorkOrder>> k = allocationDateMap.ceilingEntry(startTime);
    	Entry<Long, List<WorkOrder>> nextEntry = allocationDateMap.higherEntry(startTime);
    	Entry<Long, List<WorkOrder>> afterNextEntry = nextEntry == null ? null : allocationDateMap.higherEntry(nextEntry.getKey());
    	Entry<Long, List<WorkOrder>> prevEntry = allocationDateMap.lowerEntry(startTime);
    	Entry<Long, List<WorkOrder>> beforePrevEntry = prevEntry == null ? null : allocationDateMap.lowerEntry(prevEntry.getKey());
    	List<WorkOrder> ex = null;
    	
		/* key must exist...
		 * remove score btw k and prev, btw k and before previous, and btw next and k, afterNext and k
		 * remove the wo 
		 * if key remains : add score  btw k and prev, btw k and beforePrev, btw next and k, afterNext and k
		 * if key can be removed: (no need : remove score btw next and prev, add score next and prev,) next and before prev, afterNext previous
		 */
		ex = k.getValue();
		//Entry<Long, List<WorkOrder>> nextEntryAfter = allocationDateMap.higherEntry(startTime);
		if(beforePrevEntry != null)
			mScore = mScore.subtract(score(ex, beforePrevEntry.getValue(), k.getKey() - beforePrevEntry.getKey()));
		if(prevEntry != null)
			mScore = mScore.subtract(score(ex, prevEntry.getValue(), k.getKey() - prevEntry.getKey()));
		if(nextEntry != null)
			mScore = mScore.subtract(score(nextEntry.getValue(), ex, nextEntry.getKey() - k.getKey()));
		if(afterNextEntry != null)
			mScore = mScore.subtract(score(afterNextEntry.getValue(), ex, afterNextEntry.getKey() - k.getKey()));
		
		ex.remove(wo);
		
		if(ex.size() == 0){
			//System.out.println("removing key "+ startTime);			
    		allocationDateMap.remove(startTime);
			if(nextEntry != null && beforePrevEntry != null)
				mScore = mScore.add(score(nextEntry.getValue(), beforePrevEntry.getValue(), nextEntry.getKey() - beforePrevEntry.getKey()));
			if(afterNextEntry != null && prevEntry != null)
				mScore = mScore.add(score(afterNextEntry.getValue(), prevEntry.getValue(), afterNextEntry.getKey() - prevEntry.getKey()));
    		
		}else{

			if(beforePrevEntry != null)
				mScore = mScore.add(score(k.getValue(), beforePrevEntry.getValue(), k.getKey() - beforePrevEntry.getKey()));
			if(prevEntry != null)
				mScore = mScore.add(score(k.getValue(), prevEntry.getValue(), k.getKey() - prevEntry.getKey()));
			if(nextEntry != null)
				mScore = mScore.add(score(nextEntry.getValue(), k.getValue(), nextEntry.getKey() - k.getKey()));
			if(afterNextEntry != null)
				mScore = mScore.add(score(afterNextEntry.getValue(), k.getValue(), afterNextEntry.getKey() - k.getKey()));

		}
		
	
    	
    	/*
    	List<WorkOrder> ex = allocationDateMap.get(startTime);
    	assert(ex != null);
    	assert(ex.contains(wo));

    	Entry<Long, List<WorkOrder>> prevEntry = allocationDateMap.lowerEntry(startTime);
        if(prevEntry != null){
        	mScore += score(ex, prevEntry.getValue());
        }
    	
    	ex.remove(wo);
    	
    	if(ex.size() == 0)    	
    		allocationDateMap.remove(startTime);
    	
        
        if(prevEntry != null){
        	Entry<Long, List<WorkOrder>> nextEntry = allocationDateMap.ceilingEntry(startTime);
        	if(nextEntry != null)
        		mScore -= score(nextEntry.getValue(), prevEntry.getValue());
        	
        }
    	*/
        
    }
    
    private List<ExecutionGroup> currGroups = new ArrayList<ExecutionGroup>(10);
    private List<ExecutionGroup> prevGroups = new ArrayList<ExecutionGroup>(10);
    
    /**
     * The score increases with the number of common components between lists
     * @param curr
     * @param prev
     * @return
     */
    private Score score(List<WorkOrder> curr, List<WorkOrder> prev, long distance){
    	int hardScore = 0;
    	int softScore = 0;
    	/*
    	List<WorkOrder> tmp = new ArrayList<WorkOrder>(1);
    	tmp.add(curr.get(0));
    	curr = tmp;
    	tmp = new ArrayList<WorkOrder>(1);
    	tmp.add(prev.get(0));
    	prev = tmp;
    	*/
    	currGroups.clear();
    	prevGroups.clear();
    	
    	for(WorkOrder wo : curr){
    		currGroups.addAll(wo.getExecutionGroups());
    	}
    	
    	for(WorkOrder wo : prev){
    		prevGroups.addAll(wo.getExecutionGroups());
    	}
    	
    	int w = 0;
    	for(ExecutionGroup g : currGroups){
    		//assert(g.getUsageCount() > 0);
    		//w += 1;
    	}
    	for(ExecutionGroup g : prevGroups){
    		//assert(g.getUsageCount() > 0);
    		if(currGroups.contains(g)){
    			w += 1;
    		}
    		//else
    		//	w += 1;
    	}
    	
    	softScore += w;
    	
    	List<StockItem> testCurr = new ArrayList<StockItem>(10);
    	for(WorkOrder wo : curr){
	    	for(StockItemTransaction t : wo.getProducedTransactionList()){
	    		testCurr.add(t.getItem());
			}
    	}
    	//w = testCurr.size();
    	w = 0;
    	for(WorkOrder wo : prev){
	    	for(StockItemTransaction t : wo.getProducedTransactionList()){
	    		if(testCurr.contains(t.getItem()))
	    			w++;
	    		//else
	    		//	w++;
			}
    	}
    	softScore += 100*w;
    	
    	//StringBuilder sb = new StringBuilder();
    	//sb.append("curr:");
    	testCurr.clear();
    	w = 0;
    	for(WorkOrder wo : curr){
	    	for(StockItemTransaction t : wo.getRequiredTransaction()){
	    		testCurr.add(t.getItem());
	    		StockItem it = t.getItem();
			}
    	}
    	w = 0;
    	//sb.append("\r\nprev:");
    	for(WorkOrder wo : prev){
    		boolean WCS_PIN = false;
			for(ExecutionGroup g : wo.getExecutionGroups()){
				if("WCS_PIN".equals(g.getCode())){ //WST_LINPINT
					WCS_PIN = true;
					break;
				}
			}
			
	    	for(StockItemTransaction t : wo.getRequiredTransaction()){
	    		
	    		StockItem it = t.getItem();
	    		//sb.append(it.getReference());
	    		//sb.append(',');
	    		int z = 1;
	    		//if("MATPR".equals(it.getCategory())){
	    		if(it.getReocod() != 3){
	    			z = 100;
	    			if(WCS_PIN)
	    				z = 1000;
	    		}
	    		if(testCurr.contains(t.getItem()))
	    			w+= z;
	    		//else
	    		//	w+= z;
			}
    	}
    	
    	//sb.append("\r\nscore:");
    	//sb.append(w);
    	//System.out.println(sb.toString());
    	distance = Math.abs(distance);
    	hardScore = (int)(w*Math.exp(-0.0002*distance));

    	
    	return Score.valueOf(hardScore, softScore);
    }

    public Score getScore() {
    	return mScore;
    }

//    public int getScorez() {
//    	
//    	if(allocationDateMap.size() == 0)
//    		return 0;
//    	
//    	mScore = 0;
//    	/*
//    	SortedSet<Long> keys = new TreeSet<Long>(allocationDateMap.keySet());
//    	Iterator<Long> it = keys.iterator();
//    	Allocation prev = allocationDateMap.get(it.next());
//    	*/
//    	
//    	
//    	Collection<List<WorkOrder>> lists = allocationDateMap.values();
//    	
//    	Iterator<List<WorkOrder>> it = lists.iterator();
//    	List<WorkOrder> prev = it.next();
//    	
//    	List<ExecutionGroup> test = new ArrayList<ExecutionGroup>();
//    	List<StockItem> testProduced = new ArrayList<StockItem>();
//    	List<ExecutionGroup> pList = new ArrayList<ExecutionGroup>();
//		List<ExecutionGroup> nList = new ArrayList<ExecutionGroup>();
//		List<StockItem> pStkList = new ArrayList<StockItem>();
//		List<StockItem> nStkList = new ArrayList<StockItem>();
//    	while(it.hasNext()){
//    		List<WorkOrder> nxt = it.next();
//    		//assert(prev.size() > 0);
//    		assert(nxt.size() > 0);
//    		
//    		
//    		//List<ExecutionGroup> pList = prev.getExecutionGroups();
//    		//List<ExecutionGroup> nList = nxt.getExecutionGroups();
//    		
//
//    		for(WorkOrder w : nxt){
//	    		for(ExecutionGroup g : w.getExecutionGroups()){
//	    			if(!nList.contains(g))
//	    				nList.add(g);
//	    		}
//    		}
//    		for(WorkOrder w : prev){
//        		for(ExecutionGroup g : w.getExecutionGroups()){
//        			if(!pList.contains(g))
//        				pList.add(g);
//        		}    			
//    		}
//
//    		
//    		test.addAll(pList);
//    		test.removeAll(nList);
//    		
//    		mScore -= test.size();
//    		test.clear();
//    		
//    		
//    		
//    		
//    		//extra weight if item changes
//    		//add all the items next will consume
//    		for(WorkOrder w : nxt){
//	    		for(StockItemTransaction t : w.getRequiredTransaction()){
//	    			if(!nStkList.contains(t.getItem()))
//	    				nStkList.add(t.getItem());
//	    		}
//    		}
//    		for(WorkOrder w : prev){
//        		for(StockItemTransaction t : w.getRequiredTransaction()){
//        			if(!pStkList.contains(t.getItem()))
//        				pStkList.add(t.getItem());
//        		}    			
//    		}
//    		
//    		testProduced.addAll(pStkList);
//    		testProduced.removeAll(nStkList);
//    		
//    		mScore -= 100*testProduced.size();
//    		testProduced.clear();
//    		
//    		//mScore -= nxt.size();
//    		
//    		prev = nxt;
//    	}
//    	
//    	//int volatileScore = mVolatileScore;
//    	//mVolatileScore = 0;
//        //return mScore + volatileScore;
//    	return mScore;
//    }
//    
	
}
