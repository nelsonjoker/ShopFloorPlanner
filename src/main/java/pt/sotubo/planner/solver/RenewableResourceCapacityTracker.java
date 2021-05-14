package pt.sotubo.planner.solver;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Resource;


public class RenewableResourceCapacityTracker extends ResourceCapacityTracker {

    protected int capacityEveryDay;
    protected int[] dailyCapacity;
    private List<ValuePair<Long,Long>> mDailyIdlePeriods;
    
    protected Map<Long, Integer> usedPerDay;
    protected long hardScore;
    protected long softScore;

    public RenewableResourceCapacityTracker(Resource resource) {
        super(resource);
        capacityEveryDay = resource.getCapacity();
        dailyCapacity = resource.getDailyCapacity();
        usedPerDay = new HashMap<Long, Integer>();
        hardScore = 0;
        softScore = 0;
        mDailyIdlePeriods = new ArrayList<ValuePair<Long,Long>>(2);
        mDailyIdlePeriods.add(new ValuePair<Long, Long>((long)(0), (long)(8.0*3600)));//TODO will not work after midnight
        mDailyIdlePeriods.add(new ValuePair<Long, Long>((long)(12.5*3600), (long)(14*3600)));
        mDailyIdlePeriods.add(new ValuePair<Long, Long>((long)(18*3600), (long)(24*3600))); //we are breaking after 4 hours... in order for it to fit from 14 - 18
        
    }


    @Override
    public void insert(int resourceRequirement, Operation op) {
    	
    	assert(this.resource.equals(op.getResource()));
    	assert(resourceRequirement > 0);
    	
        long startDate = op.getStartDate()/(3600*24);
        long endDate = op.getEndDate()/(3600*24);
        long startTime = op.getStartDate() - startDate*24*3600;
        long endTime = startTime + op.getDuration();
        int requirement = resourceRequirement; //op.getDuration();
        int requirementOffPeriod = 0; 
        for(ValuePair<Long, Long> i : mDailyIdlePeriods){
        	if(startTime >= i.getLeft() && endTime <= i.getRight()){
        		requirementOffPeriod = requirement;
        		break;
        	}
        }
        if(requirementOffPeriod < requirement){
	        for(long t = startTime; t < endTime ; t++){
	            for(ValuePair<Long, Long> i : mDailyIdlePeriods){
	            	if( t >= i.getLeft() && t <= i.getRight()){
	            		requirementOffPeriod++;
	            		break;
	            	}
	            }
	        }
        }
        assert(requirementOffPeriod <= requirement);

        softScore -=  requirementOffPeriod;
        
        
        //we'll distribute it amongst the days by using up all capacity available
        //if after traversing all days we still need to alocate requirement
        //we allocate it in the first day         
        for (long i = startDate; i < endDate; i++) {
            Integer used = usedPerDay.get(i);
            if (used == null) {
                used = 0;
            }
            
            
            int available = capacityEveryDay; 
        	int req = Math.min(requirement, available);
        	
        	//check if hard score will be worsened
        	used += req;        	
        	requirement -= req;
        	usedPerDay.put(i, used);
            /*
            if (used > capacityEveryDay) {
                hardScore += (used - capacityEveryDay);
            }
            used += requirement;
            if (used > capacityEveryDay) {
                hardScore -= (used - capacityEveryDay);
            }
            */
            
            if(requirement <= 0)
            	break;
        }
        
        if(requirement > 0){
        	//still need to overload...
        	//do it on the first day
        	Integer used = usedPerDay.get(startDate);
        	if (used == null) {
                used = 0;
            }
        	used += requirement;
        	usedPerDay.put(startDate, used);
        }
    }

    @Override
    public void retract(int resourceRequirement, Operation op) {
    	assert(this.resource.equals(op.getResource()));
    	
    	long startDate = op.getStartDate()/(3600*24);
        long endDate = op.getEndDate()/(3600*24);
        int requirement = resourceRequirement; //op.getDuration();
        
        long startTime = op.getStartDate() - startDate*24*3600;
        long endTime = startTime + op.getDuration();
        
        int requirementOffPeriod = 0; 
        for(ValuePair<Long, Long> i : mDailyIdlePeriods){
        	if(startTime >= i.getLeft() && endTime <= i.getRight()){
        		requirementOffPeriod = requirement;
        		break;
        	}
        }
        if(requirementOffPeriod < requirement){
	        for(long t = startTime; t < endTime ; t++){
	            for(ValuePair<Long, Long> i : mDailyIdlePeriods){
	            	if( t >= i.getLeft() && t <= i.getRight()){
	            		requirementOffPeriod++;
	            		break;
	            	}
	            }
	        }
        }
        assert(requirementOffPeriod <= requirement);
        softScore +=  requirementOffPeriod;

        
        
        //for retracting (rollback the insert) we remove the req per day
        //if after all days traversed we still have req to remove
        //remove it on the first day
        for (long i = startDate; i < endDate; i++) {
            Integer used = usedPerDay.get(i);
            if (used == null) {
                used = 0;
            }
            
            int available = capacityEveryDay; 
        	int req = Math.min(requirement, available);
        	used -= req;
        	assert(used >= 0);
        	requirement -= req;
        	usedPerDay.put(i, used);
            
            /*
            if (used > capacityEveryDay) {
                hardScore += (used - capacityEveryDay);
            }
            used -= requirement;
            if (used > capacityEveryDay) {
                hardScore -= (used - capacityEveryDay);
            }
            */
            
        	if(requirement <= 0)
            	break;
        }
        
        if(requirement > 0){
        	//still need to overload...
        	//do it on the first day
        	Integer used = usedPerDay.get(startDate);
        	if (used == null) {
                used = 0;
            }
        	used -= requirement;
        	assert(used >= 0);
        	usedPerDay.put(startDate, used);
        }
    }

    @Override
    public long getHardScore() {
    	
    	hardScore = 0;
    	Calendar cal = Calendar.getInstance();
    	cal.setTimeZone(TimeZone.getTimeZone("GMT"));
    	/*
    	Set<Long> keys = usedPerDay.keySet();
    	Iterator<Long> it = keys.iterator();
    	long firstValue = -1;
    	int firstDayIndex = -1;
    	while(it.hasNext()){
    		Long k = it.next();
    		Integer usage = usedPerDay.get(k);
    		if(firstValue < 0){
    			firstValue = k;
    			cal.setTimeInMillis(k*24*3600*1000);
    			firstDayIndex = cal.get(Calendar.DAY_OF_WEEK) - 1;
    		}
    		int idx = (int)((firstDayIndex + (k -firstValue))%7);
    		int cap = dailyCapacity[idx];
    		if(usage > cap){
    			hardScore -= (usage - cap);
    		}
    	}
    	*/
    	    	
    	for(Long s : usedPerDay.keySet()){
	        cal.setTimeInMillis(s*24*3600*1000);
	        int idx = cal.get(Calendar.DAY_OF_WEEK) - 1;	//note that Calendar.SUNDAY = 1
    		int cap = dailyCapacity[idx];
    		Integer usage = usedPerDay.get(s);
    		if(usage > cap){
    			hardScore -= (usage - cap);
    		}
    	}
    	
    	/*
    	Collection<Integer> usages = usedPerDay.values();
    	for(Integer usage : usages){
            

    		if(usage > capacityEveryDay){
    			hardScore -= (usage - capacityEveryDay);
    		}
    	}
    	*/
        return hardScore; //*10+softScore;
    }

}
