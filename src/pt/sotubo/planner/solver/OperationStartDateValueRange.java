package pt.sotubo.planner.solver;


import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.optaplanner.core.impl.domain.valuerange.AbstractCountableValueRange;
import org.optaplanner.core.impl.domain.valuerange.buildin.primint.IntValueRange;
import org.optaplanner.core.impl.domain.valuerange.buildin.primlong.LongValueRange;
import org.optaplanner.core.impl.domain.valuerange.util.ValueRangeIterator;
import org.optaplanner.core.impl.solver.random.RandomUtils;

import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.WorkOrder;


/**
 * provides possible start times for allocations
 * @author Nelson
 */
public class OperationStartDateValueRange extends AbstractCountableValueRange<Long>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3159310966481675821L;
	//private ExecutionMode mResource;
	//private Allocation mAllocation;
	private int mWindowSize;
	private int incrementUnit;
	private Long mFixedStartTime;
	private Long mMinimumStartTime;
	//private Operation mOp;
	
	private Map<Integer, List<ValuePair<Long, Long>>> mWeeklySchedule;
	private List<ValuePair<Long, Long>> mValueBlackList;
	private int mDuration;
	
	public OperationStartDateValueRange(Operation o, Long minimumStart, int windowSize){
		super();
		incrementUnit = 1;
		//mAllocation = a;
		//mResource = executionMode;
		mWindowSize = windowSize;
		mFixedStartTime = null;
		mWeeklySchedule = null;
		mDuration = o.getDuration();
		mMinimumStartTime = minimumStart;
		
		WorkOrder wo = o.getWorkOrder();
		//mOp = o;
		
		if(wo.getExecutionGroupForcedStart() != null){
			mFixedStartTime = wo.getExecutionGroupForcedStart();
			//mFixedStartTime = System.currentTimeMillis()/1000;
		}
		
		if(wo.getExecutionGroupForcedEnd() != null){
			mWindowSize = (int) (wo.getExecutionGroupForcedEnd() - from());
		}
		
		mWeeklySchedule = new HashMap<Integer, List<ValuePair<Long, Long>>>();
		for(int i = Calendar.SUNDAY; i <= Calendar.SATURDAY; i++){
			
			if(i == Calendar.SUNDAY)
				mWeeklySchedule.put(i, null);
			else if(i == Calendar.SATURDAY)
				mWeeklySchedule.put(i, null);
			else{
				List<ValuePair<Long, Long>> mDailyValueWhiteList = new ArrayList<ValuePair<Long, Long>>(2);
				//mDailyValueWhiteList.add(new IntValueRange((int)(8.5*3600), (int)(12.5*3600)));
				//mDailyValueWhiteList.add(new IntValueRange((int)(14.0*3600), (int)(17.5*3600)));
				mDailyValueWhiteList.add(new ValuePair<Long, Long>((long)(8.5*3600), (long)(17.5*3600)));
				mWeeklySchedule.put(i, mDailyValueWhiteList);
			}
		}
		
		mValueBlackList = new ArrayList<ValuePair<Long, Long>>(0);
		
		/*
		Set<Operation> ops = o.getResource().getOperationList();
		//mValueBlackList = new ArrayList<LongValueRange>(ops.size());
		for(Operation op : ops){
			mValueBlackList.add(new ValuePair<Long, Long>(op.getStartDate(), op.getEndDate()));
		}
		*/
	}
	

	private long from(){
		long from = System.currentTimeMillis()/1000;
		if(mMinimumStartTime != null)
			from = mMinimumStartTime;
		if(mFixedStartTime != null)
			from = mFixedStartTime;
		return from;
		//return mFixedStartTime == null ? System.currentTimeMillis()/1000 : mFixedStartTime;
	}
	
    @Override
    public long getSize() {
        return (mWindowSize) / incrementUnit;
    }

    @Override
    public boolean contains(Long value) {
    	long n = from();
    	long from = n;
    	long to = from + mWindowSize;
        if (value == null || value < from || value >= to) {
            return false;
        }
        
        Long endVal = value + mDuration;
        
        for(ValuePair<Long, Long> black : mValueBlackList){
        	if(black.intersects(value, endVal))
        		return false;
        }
        
        
        if(mWeeklySchedule != null){
	        Calendar cal = Calendar.getInstance();
	        cal.setTimeInMillis(value*1000);
	        int d = cal.get(Calendar.DAY_OF_WEEK);
	        List<ValuePair<Long, Long>> mDailyValueWhiteList = mWeeklySchedule.get(d);
	        if(mDailyValueWhiteList == null)
	        	return false;
	        else{
	        	boolean whitelisted = false;
	        	long dValue = (long)(value % (24*3600));
	        	long dValueEnd = dValue + mDuration;
	        	for(ValuePair<Long, Long> r : mDailyValueWhiteList){
	        		if(r.intersects(dValue, dValueEnd)){
	        			whitelisted = true;
	        			break;
	        		}
	        	}
	        	if(!whitelisted){
	        		return false;
	        	}
	        }
        }
        
        
        
        if (incrementUnit == 1) {
            return true;
        }
        return ((long) value - (long) from) % incrementUnit == 0;
    }

    @Override
    public Long get(long index) {
    	
        if (index < 0L || index >= getSize()) {
            throw new IndexOutOfBoundsException("The index (" + index + ") must be >= 0 and < size (" + getSize() + ").");
        }
        return (index * incrementUnit + from());
    }

    @Override
    public Iterator<Long> createOriginalIterator() {
    	long n = from();
        return new OriginalLongValueRangeIterator(n, n + mWindowSize);
    }

    private class OriginalLongValueRangeIterator extends ValueRangeIterator<Long> {

    	public OriginalLongValueRangeIterator(long from, long to){
    		upcoming = from;
    		this.to = to;
    	}
    	
        private long upcoming;
        private long to;

        @Override
        public boolean hasNext() {
            return upcoming < to;
        }

        @Override
        public Long next() {
            if (upcoming >= to) {
                throw new NoSuchElementException();
            }
            long next = upcoming;
            upcoming += incrementUnit;
            return next;
        }

    }

    @Override
    public Iterator<Long> createRandomIterator(Random workingRandom) {
        return new RandomLongValueRangeIterator(workingRandom);
    }

    private class RandomLongValueRangeIterator extends ValueRangeIterator<Long> {

        private final Random workingRandom;
        private final long size;
        private final long start;

        public RandomLongValueRangeIterator(Random workingRandom) {
            this.workingRandom = workingRandom;
            this.size = getSize();
            this.start = from();
        }

        @Override
        public boolean hasNext() {
            return this.size > 0L;
        }

        @Override
        public Long next() {
        	
        	
        	
        	long r = 0;
        	do{
        		long index = RandomUtils.nextLong(workingRandom, this.size);
        		r = (index * incrementUnit + this.start);
        	}while(!contains(r));
        	/*
        	if(mFixedStartTime != null){
        		assert(r >= 1451001600 && r <= 1451174399);
        		System.out.println(r);
        	}
        	*/
        	/*
        	if(r == mOp.getStartDate()){
        		System.out.println("No change in value");
        	}
        	*/
            return r;
        }

    }

}
