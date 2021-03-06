package com.joker.planner.solver;

import java.util.Collection;
import java.util.Map.Entry;

import com.joker.planner.domain.StockItem;
import com.joker.planner.domain.StockItemProductionTransaction;
import com.joker.planner.domain.StockItemTransaction;
import com.joker.planner.domain.WorkOrder;

import java.util.TreeMap;

public class StockItemCapacityTracker implements IStockItemCapacityTracker {

	private static final int RESOLUTION = 10000;
	
    protected long initialAmount;

    //protected Map<Long, Integer> availablePerPeriod;
    protected TreeMap<Long, Long> availablePerPeriod;
    private StockItem mItem;
    

    public StockItemCapacityTracker(StockItem item, float amt) {
        initialAmount = (long) (RESOLUTION*amt);
        //assert(initialAmount == RESOLUTION*amt);
        mItem = item;
    	//initialAmount = amt;
        availablePerPeriod = new TreeMap<Long, Long>();
        availablePerPeriod.put(0L, initialAmount);
        //System.out.println("StockItemCapacityTracker("+item+")" + this.hashCode());
    }

    /**
     * Get the last date when there was an available stock of minStock
     * @param minStock
     * @return
     */
    public long lastAvailabilityDate(long from, double minStock){
    	
    	double q = 0;
    	minStock *= RESOLUTION;
    	long t = -1;
    	for(long k : availablePerPeriod.keySet()){
    		if(k > from)
    			break;
    		
    		q += availablePerPeriod.get(k);
    		if(q >= minStock){
    			t = k;
    			break;
    		}
    	}
    	
    	return t;
    	
    }
    
    /**
     * Get the first time when the minStock qty will become available
     * @param from get time from this start point
     * @param minStock
     * @return
     */
    public long nextAvailabilityDate(long from, double minStock){
    	
    	double q = 0;
    	minStock *= RESOLUTION;
    	long t = -1;
    	for(long k : availablePerPeriod.keySet()){
    		
    		q += availablePerPeriod.get(k);
    		
    		if(k < from)
    			continue;
    		if(q >= minStock){
    			t = k;
    			break;
    		}
    	}
    	
    	return t;
    	
    }
    /**
     * get the next time when stock is bellow value 
     * @param from
     * @param maxStock
     * @return
     */
    public long nextUnAvailabilityDate(long from, double maxStock){
    	
    	double q = 0;
    	maxStock *= RESOLUTION;
    	long t = -1;
    	for(long k : availablePerPeriod.keySet()){
    		
    		q += availablePerPeriod.get(k);
    		
    		if(k < from)
    			continue;
    		if(q <= maxStock){
    			t = k;
    			break;
    		}
    	}
    	
    	return t;
    	
    }
    
    
    /* (non-Javadoc)
	 * @see com.joker.planner.solver.IStockItemCapacityTracker#insertRequirement(com.joker.planner.domain.StockItemTransaction, com.joker.planner.domain.WorkOrder)
	 */
	public void insertRequirement(StockItemTransaction resourceRequirement, WorkOrder wo) {
    	
    	assert(resourceRequirement.getItem().equals(mItem));
    	
        long startDate = wo.getFirstStart();
        long consumed = (long)(RESOLUTION*resourceRequirement.getQuantity());
        //float consumed = resourceRequirement.getQuantity();
        //assert(consumed == RESOLUTION*resourceRequirement.getQuantity());
        if(consumed == 0)
        	return;
        assert(consumed > 0);
        /*
        //compute availability at the end of period
        int avail = 0;
        Set<Long> periods = availablePerPeriod.keySet();
        for(Long n : periods){
        	if(n <= startDate){
	        	avail += availablePerPeriod.get(n);
        	}
        }
        */
        
        //register transaction
        Long a = availablePerPeriod.get(startDate);
        if(a == null){
        	a = 0L;
        }
        
        a = a - consumed;
        
        
        availablePerPeriod.put(startDate, a);
        /*
         * cannot remove because the requirement may zero out the available
        if(a == 0){
        	availablePerPeriod.remove(startDate);
        }else{
        	availablePerPeriod.put(startDate, a);
        }
        */
        
        //System.out.println("IR: "+this.toString());
    }
    
    /* (non-Javadoc)
	 * @see com.joker.planner.solver.IStockItemCapacityTracker#insertProduction(com.joker.planner.domain.StockItemTransaction, com.joker.planner.domain.WorkOrder)
	 */
	public void insertProduction(StockItemTransaction resourceProduction, WorkOrder wo) {
    	
    	assert(resourceProduction instanceof StockItemProductionTransaction);
    	
        long endDate = wo.getLastEnd();
        long produced = (long)(RESOLUTION*resourceProduction.getQuantity());
        //assert(produced == RESOLUTION*resourceProduction.getQuantity());
        //float produced = resourceProduction.getQuantity();
        if(produced == 0)
        	return;
        assert(produced > 0);
        //register transaction
        Long a = availablePerPeriod.get(endDate);
        if(a == null){
        	a = 0L;
        }
        
        a = a + produced;
        
        
        availablePerPeriod.put(endDate, a);
        /*
         * cannot remove because the production may zero out the available
        if(a == 0){
        	availablePerPeriod.remove(endDate);
        }else{
        	availablePerPeriod.put(endDate, a);
        }
        */
        
        //System.out.println("IP: "+this.toString());
    }

    /* (non-Javadoc)
	 * @see com.joker.planner.solver.IStockItemCapacityTracker#retractRequirement(com.joker.planner.domain.StockItemTransaction, com.joker.planner.domain.WorkOrder)
	 */
	public void retractRequirement(StockItemTransaction resourceRequirement, WorkOrder wo) {
    	
    	long startDate = wo.getFirstStart();
        //long endDate = allocation.getEndDate();
        long consumed = (long)(RESOLUTION*resourceRequirement.getQuantity());
        //assert(consumed == RESOLUTION*resourceRequirement.getQuantity());
    	//float consumed = resourceRequirement.getQuantity();
        if(consumed == 0)
        	return;
        
        assert(consumed > 0);
        //int produced = resourceRequirement.getProducedQuantity();
        
        //register transaction
        Long a = availablePerPeriod.get(startDate);
        //cannot assert a exists because the insert may actually make it 0 and thus remove it
        //assert(a != null);
        if(a == null)
        	a = 0L;
       
        a += consumed;
        
        availablePerPeriod.put(startDate, a);
        /*
        if(a == 0)
        	availablePerPeriod.remove(startDate);
        else
        	availablePerPeriod.put(startDate, a);
        */
        //System.out.println("RR: "+this.toString());
    }
    
    /* (non-Javadoc)
	 * @see com.joker.planner.solver.IStockItemCapacityTracker#retractProduction(com.joker.planner.domain.StockItemTransaction, com.joker.planner.domain.WorkOrder)
	 */
	public void retractProduction(StockItemTransaction resourceProduction, WorkOrder wo) {
    	
    	assert(resourceProduction instanceof StockItemProductionTransaction);
    	
        long endDate = wo.getLastEnd();
        long produced = (long)(RESOLUTION*resourceProduction.getQuantity());
        //assert(produced == RESOLUTION*resourceProduction.getQuantity());
        //float produced = resourceProduction.getQuantity();
        if(produced == 0)
        	return;
        
        assert(produced > 0);
        //register transaction
        Long a = availablePerPeriod.get(endDate);
        assert(a != null);
        if(a == null)
        	a = 0L;
       
        a -= produced;
        
        availablePerPeriod.put(endDate, a);
        /*
        if(a == 0)
        	availablePerPeriod.remove(endDate);
        else
        	availablePerPeriod.put(endDate, a);
        */
        //System.out.println("RP: "+this.toString());
    }

    /* (non-Javadoc)
	 * @see com.joker.planner.solver.IStockItemCapacityTracker#getHardScore()
	 */
	public int getHardScore() {
    	int hardScore = 0;
    	
    	int weight = 1;
    	/*
    	if(mItem.getReocod() == 3){	//our main focus is on produced items
        	weight = 10;
        }
    	*/
    	Collection<Long> avails = availablePerPeriod.values();
    	assert(availablePerPeriod.containsKey(0L));
    	assert(availablePerPeriod.get(0L) == initialAmount);
    	//Iterator<Allocation> it = allocs.iterator();
    	//Allocation prev = it.next();
    	
    	//SortedSet<Long> keys = new TreeSet<Long>(availablePerPeriod.keySet());
    	int v = 0;
    	//for (Long key : keys) { 
    	for (Long a : avails) {
    		//v += availablePerPeriod.get(key);
    		v += a;
    		if(v < 0){
    			hardScore += v;
    			//hardScore--;
    			v = 0;
    		}
    	}
        return hardScore*weight;
    }
	
	/**
	 * the amount in stock while running
	 * @return
	 */
	public int getSoftScore() {
    	int softScore = 0;
    	
    	if(availablePerPeriod.size() == 0)
    		return 0;
    	
    	Collection<Long> avails = availablePerPeriod.values();
    	int v = 0;
    	int lastV = 0;
    	long inStockTime = availablePerPeriod.firstKey();
    	long outStockTime = inStockTime;
    	for (Long t : availablePerPeriod.keySet()) { 
    		Long a = availablePerPeriod.get(t);
    		
    		v += a;
    		if(v > 0 && lastV <= 0){
    			inStockTime = t;
    		}else if(v <= 0 && lastV > 0){
    			outStockTime = t;
    			softScore -= (outStockTime - inStockTime)/(24*3600);
    		}
    		
    		lastV = v;
    	}
        return softScore;
    }
    
    @Override
    public String toString() {
    	StringBuilder str = new StringBuilder();
    	str.append(mItem.toString());
    	str.append(' ');
    	for(Long k  : availablePerPeriod.keySet()){
    		str.append(k);
    		str.append(':');
    		str.append(availablePerPeriod.get(k));
    		str.append(' ');
    	}
    	return str.toString();
    }
    

}
