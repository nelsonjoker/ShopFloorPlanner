package com.joker.planner.solver;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.joker.planner.SortedArrayList;
import com.joker.planner.domain.Operation;
import com.joker.planner.domain.Resource;
import com.joker.planner.domain.Schedule;
import com.joker.planner.domain.StockItem;
import com.joker.planner.domain.StockItemProductionTransaction;
import com.joker.planner.domain.StockItemTransaction;
import com.joker.planner.domain.WorkOrder;

public class DeltaRuleSolver{

	public static final int THREAD_COUNT = 1;
	
	
	private Lock mBestScoreSync;
	private Score mBestScore;
	private Schedule mBestSchedule;
	private Schedule mWorkingSolution;
	private Lock mWorkingSolutionSync;
	private boolean mWorkingSolutionChanged;
	
	private Map<CacheKey, Integer> groupRankCache = null;
	private List<NewBestSolutionListener> mSolutionListeners;
	private Map<Operation, List<Resource>> lastResourceMoves;
	private Random mRandom;
	private Map<Operation, List<Long>> lastMoves;
	private Map<String, Long> mOperationTiredness;
	private long mOperationTirednessMax;
	private long mMinimumStartTime;
	private long getMinimumStartTime() { return mMinimumStartTime; }

	
	protected void setBestSchedule(Score score, Schedule sch){
		mBestScoreSync.lock();
		try{
			mBestScore = score;
			mBestSchedule = sch;
			mBestSchedule.setScore(score.multiply(1.0));
			System.out.println(Thread.currentThread().getId()+" > "+score.toString());
			
			for(NewBestSolutionListener l : mSolutionListeners){
				l.OnNewBestSolution(this, score);
			}
			
			
			
		}finally{
			mBestScoreSync.unlock();
		}
	}
	
	public Schedule getBestScheduleClone(){
		Schedule cl = null;
		mBestScoreSync.lock();
		try{
			if(mBestSchedule != null){
				cl = mBestSchedule.planningClone();
			}
		}finally{
			mBestScoreSync.unlock();
		}
		return cl;
	}
	
	public interface NewBestSolutionListener{
		void OnNewBestSolution(DeltaRuleSolver solver, Score score);
	}
	public synchronized void addNewBestSolutionListener(NewBestSolutionListener listener){
		mSolutionListeners.add(listener);
	}
	
	
	public DeltaRuleSolver(Schedule workingSolution) {
		mBestScore = null;
		mBestSchedule = null;
		mBestScoreSync = new ReentrantLock();
		mWorkingSolutionSync = new ReentrantLock();
		mWorkingSolution = workingSolution;
		mWorkingSolutionChanged = true;
		
		mSolutionListeners = new ArrayList<>();
		lastMoves = new HashMap<Operation, List<Long>>();
		mOperationTiredness = new HashMap<>();
		mOperationTirednessMax = 0;
		lastResourceMoves = new HashMap<Operation, List<Resource>>();
		mRandom = new Random(System.currentTimeMillis());
		mMinimumStartTime = ((System.currentTimeMillis()/1000)/(24*3600))*24*3600 - 1*30*24*3600;
	}

	public void solve(){
		run();
	}
	
	public void run() {
		
		mWorkingSolutionSync.lock();
		
		Schedule sch = mWorkingSolution;
		
		OperationIncrementalScoreCalculator calc = new  OperationIncrementalScoreCalculator();
		
		boolean mod = false;
		int dbgCounter = 0;
		int dbgResCounter = 0;
		int iteration = 0;
		Calendar cal = Calendar.getInstance();
    	cal.setTimeZone(TimeZone.getTimeZone("GMT"));
    	SortedArrayList<Operation> sorted = new SortedArrayList<>(new Comparator<Operation>() {
			@Override
			public int compare(Operation o1, Operation o2) {
				return o1.getStartDate().compareTo(o2.getStartDate());
			}
		});
    	mWorkingSolutionChanged = true;
    	Score bestLocalScore = null;
    	Score score = null;    
    	List<Operation> ops = null;
    	Map<String, List<WorkOrder>> invoiceMap = null;
    	List<WorkOrder> allWo = null;
    	List<Resource> res = null;
    	Map<Resource, List<Operation>> resourceOperationMap = null;
    	long now = System.currentTimeMillis() / 1000;
    	long updateCounter = 0;
    	
    	Map<Operation, Double> operationGradients = new HashMap<>();
    	Map<Operation, Double> operationMu = new HashMap<>();
    	
    	
		do{
			mWorkingSolutionSync.unlock();
			mWorkingSolutionSync.lock();
			
			if(mWorkingSolutionChanged){
				
				
				
				
				ops = sch.getOperationList();
				invoiceMap = new HashMap<String, List<WorkOrder>>();
				allWo = sch.getWorkOrderList();
				for(WorkOrder wo  : allWo){
					for (StockItemProductionTransaction stockProduction : wo.getProducedTransactionList()) {
						List<WorkOrder> woList = invoiceMap.get(stockProduction.getVCR());
						if(woList == null){
							woList = new LinkedList<>();
							invoiceMap.put(stockProduction.getVCR(), woList);
						}
						woList.add(wo);
					}
					
					
					long min = Long.MAX_VALUE;
					long max = Long.MIN_VALUE;
					for(Operation op : wo.getOperations()){
						if(op.getStartDate() < min)
							min = op.getStartDate();
						if(op.getEndDate() > max)
							max = op.getEndDate();
					}
					if(min < Long.MAX_VALUE || max > Long.MIN_VALUE){
						//as long as there is one operation, there will be both min and max
						if(min != 1*wo.getFirstStart() || max != 1*wo.getLastEnd()){
							wo.setFirstStart(min);
							wo.setLastEnd(max);					
						}
					}
				}
				
				calc.resetWorkingSolution(sch);
				
				updateNextOperations(ops, allWo, invoiceMap);
				invoiceMap.clear();
				
				
				
				
				res = sch.getResourceList();
				resourceOperationMap = new HashMap<Resource, List<Operation>>();
				for(Resource r : res){
					resourceOperationMap.put(r, new ArrayList<Operation>());
				}
				for(Operation o : ops){
					resourceOperationMap.get(o.getResource()).add(o);
					
					operationGradients.put(o, mRandom.nextDouble()/1000.0);
					operationMu.put(o, 0.0);
				}
				
				
				
				mOperationTiredness.clear();
				mOperationTirednessMax = 0;
				groupRankCache = null;
				mWorkingSolutionChanged = false;
				
				score = calc.calculateScore();
				now = System.currentTimeMillis() / 1000;
			}
			
			iteration++;
			System.out.println("Starting iteration " + iteration + " -------------------------------- ");
			
			
			dbgCounter = 0;
			for(Operation o : ops){
				long start = o.getStartDate();
				double grad = operationGradients.get(o);
				//grad += grad*mRandom.nextGaussian()/10;
						
				double mu = operationMu.get(o);
				start = (long) (start + o.getDuration()*(grad + mu));
				if(start != o.getStartDate()){
					doMove(calc,o,start);
					dbgCounter ++;
				}
			}
			
			Score oldScore = score;
			score = calc.calculateScore();
			int compare = score.compareTo(oldScore);
			compare = (int) (Math.signum(compare)*Math.min(Math.abs(compare), 1000));
			if(compare != 0){
				for(Operation o : ops){
					double grad = operationGradients.get(o);
					grad += 0.0001*compare;
					operationGradients.put(o, grad);
					double mu = operationMu.get(o);
					mu += 0.00001*grad;
					operationMu.put(o, mu);
				}
			}
			
			
			System.out.println(iteration + " Current score "+score);
			if(bestLocalScore == null || score.compareTo(bestLocalScore) > 0){
				System.out.println("New best local score " + score);
				bestLocalScore = score;
			}
			
			
			if(mBestScoreSync.tryLock()){
				try{
					if(mBestScore == null || score.compareTo(mBestScore) > 0)
					{
						mBestScore = score;
						setBestSchedule(mBestScore, sch);
					}
				}finally {
					mBestScoreSync.unlock();
				}
			}
			
			
			//iteration++;
		}while(true);
		
		
	}
	
	
	private void log(String message){
		System.out.println(Thread.currentThread().getId()+" > " +  message);
	}
	
	
	protected boolean acceptMove(Operation o, Long target){
		if(1*target == o.getStartDate())
			return false;
		if(target < getMinimumStartTime())
			return false;
		/*
		WorkOrder wo = o.getWorkOrder();
		if(wo.getExecutionGroupForcedEnd() != null && target + o.getDuration() > wo.getExecutionGroupForcedEnd())
			return false;
		if(wo.getExecutionGroupForcedStart() != null && target < wo.getExecutionGroupForcedStart())
			return false;
		*/
		
		Long t = mOperationTiredness.getOrDefault(o.getCode(), 0L);
		
		mOperationTirednessMax =  Math.max(mOperationTirednessMax, t);
		if(10.0*(t/(double)mOperationTirednessMax) < Math.abs(mRandom.nextGaussian()))
		//if((mRandom.nextDouble() * mOperationTirednessMax + 0.5) >= 0.5*t)
			return true;
		
		return false;
		
		
//		List<Long> os = lastMoves.get(o);
//		if(os == null){
//			os = new ArrayList<Long>(1000);
//			lastMoves.put(o, os);
//		}
//		
//		
//		if(os.size() > 0){
//			if(mRandom.nextDouble() < 0.90)
//				os.remove(0);
//		}
//		if(os.size() > 0)
//			return false;
//		os.add(target);
//		return true;
//		
		
//		if(os.size() > 0 && (mRandom.nextDouble() > 0.90 || os.size() > 10)){
//		//while(os.size() > 30){
//			os.remove(mRandom.nextInt(os.size()));
//		}
//		boolean res = !os.contains(target);
//		//by allowing duplicates we make sure the targets most used remain in the list the most time
//		//thus disallowing it's usage
//		//if(res)
//			os.add(target);
//		return res;
//		//return true;
	}
	
	protected boolean acceptMove(Operation o, Resource target){
		List<Resource> os = lastResourceMoves.get(o);
		if(os == null){
			os = new ArrayList<Resource>(10);
			lastResourceMoves.put(o, os);
		}
		int count = 0;
		for(Resource r: os){
			if(r.equals(target))
				count++;
		}
		os.add(target);
		if(os.size() > 10){
			os.remove(mRandom.nextInt(os.size()));
		}
		
		return count < 5;
		//return true;
		
	}
	
	protected void doMove(OperationIncrementalScoreCalculator calc, Operation o, long targetStart){
		
		
		Long t = mOperationTiredness.get(o.getCode());
		if(t == null){
			t = 1L;
		}else if (t < 1e50){
			t++;
		}
		mOperationTiredness.put(o.getCode(), t);
		
		calc.beforeVariableChanged(o, "startDate");
		o.setStartDate(targetStart);
		calc.afterVariableChanged(o, "startDate");

		WorkOrder wo = o.getWorkOrder();
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		for(Operation op : wo.getOperations()){
			if(op.getStartDate() < min)
				min = op.getStartDate();
			if(op.getEndDate() > max)
				max = op.getEndDate();
		}
		if(min < Long.MAX_VALUE || max > Long.MIN_VALUE){
			//as long as there is one operation, there will be both min and max
			if(min != 1*wo.getFirstStart() || max != 1*wo.getLastEnd()){					
				calc.beforeVariableChanged(wo, "firstStart");
				wo.setFirstStart(min);
				wo.setLastEnd(max);					
				calc.afterVariableChanged(wo, "firstStart");
			}
		}
					
		
		
		
	}
	
	
	
	protected void doMove(OperationIncrementalScoreCalculator calc, Operation o, Resource res){
		
		calc.beforeVariableChanged(o, "resource");
		
		o.getResource().getOperationList().remove(o);		
		o.setResource(res);
		assert(!res.getOperationList().contains(o));
		res.getOperationList().add(o);
		calc.afterVariableChanged(o, "resource");
	}
	

	private class CacheKey {

	    private final int x;
	    private final int y;
	    

	    public CacheKey(int x, int y) {
	        this.x = x;
	        this.y = y;
	    }

	    @Override
	    public boolean equals(Object o) {
	        if (this == o) return true;
	        if (!(o instanceof CacheKey)) return false;
	        CacheKey key = (CacheKey) o;
	        return (Objects.equals(x, key.x) && Objects.equals(y, key.y)) || (Objects.equals(x, key.y) && Objects.equals(y, key.x));
	    }

	    @Override
	    public int hashCode() {
	    	if(x < y)
	    		return new HashCodeBuilder().append(x).append(y).toHashCode();
	    	else
	    		return new HashCodeBuilder().append(y).append(x).toHashCode();
	    }

	}
	
	
	protected int operationGroupRank(List<Operation> ops, int a, int b){
		
		CacheKey k = new CacheKey(a, b);
		if(groupRankCache == null){
		
			groupRankCache = new HashMap<CacheKey, Integer>();
			
			//int size = ops.size();
			//byte[][] operationGroups = new byte[size][size];
			
			for(int i = 0; i < ops.size(); i++)
			{
				Operation o1 = ops.get(i);
				List<StockItem> consumedPrev = new ArrayList<StockItem>(10);
				List<StockItem> producedPrev = new ArrayList<StockItem>(10);
				WorkOrder wo = o1.getWorkOrder();
				
				for(StockItemTransaction tr : wo.getRequiredTransaction()){
					StockItem it = tr.getItem();
					consumedPrev.add(it);
				}
				for(StockItemTransaction tr : wo.getProducedTransactionList()){
					producedPrev.add(tr.getItem());
				}
				int bestScore = 0;
				for(int j = i + 1; j < ops.size(); j++)
				{
					Operation o2 = ops.get(j);
				
					if(!o1.getResourceRange().contains(o2.getResource()))
						continue;
					
					wo = o2.getWorkOrder();
					int val = 0;
					for(StockItemTransaction tr : wo.getRequiredTransaction()){
						StockItem it = tr.getItem();
						if(consumedPrev.contains(it)){
							val += it.getReocod() != 3 ? 100 : 2;
						}
					}
					
					for(StockItemTransaction tr : wo.getProducedTransactionList()){
						StockItem it = tr.getItem();
						if(producedPrev.contains(it)){
							val++;
						}
					}
					
					if(val > bestScore)
						bestScore = val;
					
					if(val > 0)
						groupRankCache.put(new CacheKey(i, j), val);
					
					//operationGroups[i][j] = (byte)(val > 127 ? 127 : val);
					//operationGroups[j][i] = (byte)(val > 127 ? 127 : val);
				}
				consumedPrev.clear();
				consumedPrev = null;
				producedPrev.clear();
				producedPrev = null;
				
				if(bestScore > 0)
					groupRankCache.put(new CacheKey(i, i), bestScore);
				//operationGroups[i][i] = (byte)(bestScore > 127 ? 127 : bestScore);
			}
		}				
		
		Integer v = groupRankCache.getOrDefault(k, 0);
		return v;

	}
	
	
	
	
	private void updateNextOperations(List<Operation> ops , List<WorkOrder> allWo, Map<String, List<WorkOrder>> invoiceMap) {
		
		
		
		//replicate the sequence as imposed by MRP for each vcr
		//first for all operations in a workorder enforce sequence
		List<Operation> terminalOps = new ArrayList<>(allWo.size());
		List<Operation> starterOps = new ArrayList<>(ops);
		for(WorkOrder w : allWo){
			List<Operation> wops = w.getOperations();
			Map<Integer, Operation> openums = new HashMap<>();
			for(Operation o : wops){
				openums.put(o.getOPENUM(), o);
			}
			for(Operation o : wops){
				int next = o.getNEXOPENUM();
				Operation n = openums.get(next);
				o.setNextOperation(n); //if null so be it
				if(n == null)
					terminalOps.add(o);
				else
					starterOps.remove(n);
			}
		}
		List<StockItemTransaction> usedTrans = new ArrayList<>(allWo.size());
		for(Operation o  : terminalOps){
			WorkOrder wo = o.getWorkOrder();
			StockItemProductionTransaction stockProduction = wo.getProducedTransactionList().get(0);
			List<WorkOrder> consumers = new LinkedList<>();
			List<StockItemTransaction> consumerTransaction = new LinkedList<>();
			for(WorkOrder c : invoiceMap.get(stockProduction.getVCR())){
				List<StockItemTransaction> reqs = c.getRequiredTransaction();
				for(StockItemTransaction r : reqs){
					if(usedTrans.contains(r))
						continue;

					if(r.getItem().equals(stockProduction.getItem()) && ! consumers.contains(c)){
						consumers.add(c);
						consumerTransaction.add(r);
					}
				}
			}
			WorkOrder consumer = null;
			StockItemTransaction consumerTrans = null;
			for(int i = 0; i < consumers.size(); i++){
				WorkOrder c = consumers.get(i);
				StockItemTransaction r = consumerTransaction.get(i);
				if(Math.abs(r.getQuantity() - stockProduction.getQuantity()) < 0.0001){
					//we have a qty match, override any previous result and use this instead
					consumer = c; consumerTrans = r;
					break;
				}else if(consumer == null){
					consumer = c; consumerTrans = r;	//hang on the first one found
				}
			}
			
			if(consumer != null){
				usedTrans.add(consumerTrans);
				for(Operation n : consumer.getOperations()){
					if(starterOps.contains(n)){
						o.setNextOperation(n);
						break;
					}
				}
			}
			
		}
		
		//any workorder with more than one operation on the same openum
		//was split and needs to be sequenced
		for(WorkOrder wo : allWo){
			
			Map<Integer, List<Operation>> openumOps = new HashMap<>(wo.getOperations().size());
			for(Operation o : wo.getOperations()){
				List<Operation> wops = openumOps.get(o.getOPENUM());
				if(wops == null){
					wops = new LinkedList<Operation>();
					openumOps.put(o.getOPENUM(), wops);
				}
				wops.add(o);				
			}
			
			for(List<Operation> wops : openumOps.values()){
				if(wops.size() <= 1)
					continue;
				
				Operation next = null;
				for(Operation o : wops){
					if(o.getNextOperation() != null){
						next = o.getNextOperation();
						break;
					}
				}
				assert(next != null);
				for(int i = wops.size() - 1 ; i >= 0; i--){
					Operation o = wops.get(i);
					o.setNextOperation(next);
					next = o;					
				}
				
			}
			
			
			
			
		}
		
		
		for(Operation o : ops){
			if(!"EXPE".equals(o.getResource().getCode())){
				if(o.getNextOperation() == null){
					log("Invalid next operation for "+o.toString());
				}
			}
		}
		
		
		
	}


	public void terminateEarly() {
		// TODO Auto-generated method stub
		
		
	}
	
	public Schedule lockWorkingSolution(){
		mWorkingSolutionSync.lock();
		return mWorkingSolution;
	}

	public void releaseWorkingSolution(){
		releaseWorkingSolution(mWorkingSolutionChanged);
	}
	
	public void releaseWorkingSolution(boolean changed){
		mWorkingSolutionChanged = changed;
		mWorkingSolutionSync.unlock();
	}


	public void doMove(Operation ex, long start) {
		// TODO Auto-generated method stub
		
	}


	public void doMove(Operation ex, Resource res) {
		// TODO Auto-generated method stub
		
	}
	
	

}
