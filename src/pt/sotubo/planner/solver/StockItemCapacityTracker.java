package pt.sotubo.planner.solver;

import java.util.Collection;
import java.util.TreeMap;

import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemProductionTransaction;
import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;

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

    /* (non-Javadoc)
	 * @see pt.sotubo.planner.solver.IStockItemCapacityTracker#insertRequirement(pt.sotubo.planner.domain.StockItemTransaction, pt.sotubo.planner.domain.WorkOrder)
	 */
    @Override
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
	 * @see pt.sotubo.planner.solver.IStockItemCapacityTracker#insertProduction(pt.sotubo.planner.domain.StockItemTransaction, pt.sotubo.planner.domain.WorkOrder)
	 */
    @Override
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
	 * @see pt.sotubo.planner.solver.IStockItemCapacityTracker#retractRequirement(pt.sotubo.planner.domain.StockItemTransaction, pt.sotubo.planner.domain.WorkOrder)
	 */
    @Override
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
	 * @see pt.sotubo.planner.solver.IStockItemCapacityTracker#retractProduction(pt.sotubo.planner.domain.StockItemTransaction, pt.sotubo.planner.domain.WorkOrder)
	 */
    @Override
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
	 * @see pt.sotubo.planner.solver.IStockItemCapacityTracker#getHardScore()
	 */
    @Override
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
