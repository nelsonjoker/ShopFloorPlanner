package com.joker.planner.solver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.joker.planner.SortedArrayList;
import com.joker.planner.domain.ExecutionGroup;
import com.joker.planner.domain.Operation;
import com.joker.planner.domain.Resource;
import com.joker.planner.domain.Schedule;
import com.joker.planner.domain.StockItem;
import com.joker.planner.domain.StockItemProductionTransaction;
import com.joker.planner.domain.StockItemTransaction;
import com.joker.planner.domain.WorkOrder;

public class BruteForceSolver extends BaseSolver{

	private static final ExecutorService THREADPOOL = Executors.newFixedThreadPool(2);
	private static long theLastRankComputeTime = 0L;
	
	private Lock mBestScoreSync;
	private Score mBestScore;
	private Schedule mBestSchedule;
	
	
	
	private OperationIncrementalScoreCalculator mCalculator;
	
	private Map<CacheKey, Integer> groupRankCache = null;
	private Object groupRankCacheSync;
	
	private volatile boolean mRunning;
	private Map<Operation, List<Long>> mLastMoves;
	private Random mRandom;
	private Map<String, Long> mOperationTiredness;
	private long mOperationTirednessMax;
	private File mGroupRankCacheFile = new File("group_rank_cache.data");
	
	protected void setBestSchedule(Score score, Schedule sch){
		mBestScoreSync.lock();
		try{
			mBestScore = score;
			mBestSchedule = sch;
			mBestSchedule.setScore(score.multiply(1.0));
			System.out.println(Thread.currentThread().getId()+" > "+score.toString());
			
			OnNewBestSolution(score);
			
			
			
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
	
	
	
	
	
	public BruteForceSolver(Schedule workingSolution) {
		super(workingSolution);
		mBestScore = null;
		mBestSchedule = null;
		mBestScoreSync = new ReentrantLock();
		
		
		mCalculator = null;
		
		
		mRandom = new Random(System.currentTimeMillis());
		mLastMoves = new HashMap<>();
		mOperationTiredness = new HashMap<>();
		mOperationTirednessMax = 0;
		groupRankCache = null;
		groupRankCacheSync = new Object();
		
		if(mGroupRankCacheFile.exists()){
			try {
				groupRankCache = (Map<CacheKey, Integer>) SerializationUtils.deserialize(new FileInputStream(mGroupRankCacheFile));
			} catch (Exception e) {
				e.printStackTrace();
				groupRankCache = null;
			}
		}
		
	}

	public void solve(){
		mRunning = true;
		run();
	}
	
	public void run() {
		
		mWorkingSolutionSync.lock();
		
		Schedule sch = mWorkingSolution;
		
		OperationIncrementalScoreCalculator calc = new  OperationIncrementalScoreCalculator();
		mCalculator = calc;
		boolean mod = false;
		int dbgCounter = 0;
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
    	Score score = null;    
    	List<Operation> ops = null;
    	Map<String, List<WorkOrder>> invoiceMap = null;
    	List<WorkOrder> allWo = null;
    	List<Resource> res = null;
    	//Map<Resource, List<Operation>> resourceOperationMap = null;
    	long updateCounter = 0;
    	long lastUpdateCounter = 0;
    	Map<Operation, Integer> opIndexes = new HashMap<>(10000);
    	Random mRandom = new Random();
    	//long lastModTime = System.currentTimeMillis();
    	int noModCounter = 0;
		do{
			mWorkingSolutionSync.unlock();
			Thread.yield();
			mWorkingSolutionSync.lock();
			
			if(mWorkingSolutionChanged || iteration == 0){	
				//on the first run we may be interrupted before getting to start
				//and we need to make sure shit gets initialized, hence iteration == 0
				
				
				
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
				
				updateNextOperations(ops, allWo, invoiceMap);
				invoiceMap.clear();
				
				calc.resetWorkingSolution(sch);
				
				Map<String, Long> filteredOperations = new HashMap<>(mOperationTiredness);
				mOperationTiredness.clear();
				mOperationTirednessMax = 0;
				
				Map<Operation, List<Long>> filteredLastMoves = new HashMap<>(mLastMoves);
				mLastMoves.clear();
				
				res = sch.getResourceList();
//				resourceOperationMap = new HashMap<Resource, List<Operation>>();
//				for(Resource r : res){
//					resourceOperationMap.put(r, new ArrayList<Operation>());
//				}
				for(Operation o : ops){
					
					assert(o.getResource().getOperationList().contains(o));
//					assert(!resourceOperationMap.get(o.getResource()).contains(o));
//					resourceOperationMap.get(o.getResource()).add(o);
					Long v = filteredOperations.get(o.getCode());
					if(v != null){
						mOperationTiredness.put(o.getCode(), v);
						if(v > mOperationTirednessMax)
							mOperationTirednessMax = v;
					}
					
					List<Long> moves = filteredLastMoves.get(o);
					if(moves != null && moves.size() > 0){
						mLastMoves.put(o, moves);
					}
					
				}
				
				opIndexes.clear();
				for(int i = 0; i < ops.size() ; i++){
					opIndexes.put(ops.get(i), i);
				}
				mod = true;
				//groupRankCache = null;
				mWorkingSolutionChanged = false;
				
				score = calc.calculateScore();
				mBestScore = null; //score;	//ensure update after solution changes
				updateCounter = 0;
		    	lastUpdateCounter = 0;
		    	boolean groupRankCacheIsNull = false;
		    	synchronized(groupRankCacheSync){
		    		groupRankCacheIsNull = groupRankCache == null;
		    	}
		    	if(iteration == 0 && groupRankCacheIsNull){
		    		theLastRankComputeTime = System.currentTimeMillis();
		    		synchronized(groupRankCacheSync){
						groupRankCache = computeGroupRankCache(ops, groupRankCache, theLastRankComputeTime);
						if(groupRankCache != null){
				    		try {
								SerializationUtils.serialize((Serializable) groupRankCache, new FileOutputStream(mGroupRankCacheFile));
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
		    		}
		    	}else{
		    	
		    		
			    	final List<Operation> cacheOps = sch.getOperationList();
			    	Map<CacheKey, Integer> c = null;
			    	synchronized(groupRankCacheSync){
			    		if(groupRankCache != null){
			    			c = new HashMap<CacheKey, Integer>(groupRankCache);
			    		}
			    	}
			    	final Map<CacheKey, Integer> cache = c;
					THREADPOOL.submit(new Runnable() {
						
						@Override
						public void run() {
							
							theLastRankComputeTime = System.currentTimeMillis();
							Map<CacheKey, Integer> nCache = computeGroupRankCache(cacheOps, cache, theLastRankComputeTime);
							if(nCache != null){
								synchronized(groupRankCacheSync){
									groupRankCache = nCache;
						    		try {
										SerializationUtils.serialize((Serializable) groupRankCache, new FileOutputStream(mGroupRankCacheFile));
									} catch (Exception e) {
										e.printStackTrace();
									}

								}
							}
							
						}
					});
		    	}
			}
			
			
			
			iteration++;
			System.out.println("Starting iteration " + iteration + " -------------------------------- ");
			lastUpdateCounter = updateCounter;
			updateCounter = 0; 
			
			
			
			do{
				dbgCounter = 0;
				SortedArrayList<Operation> deliverySorted = new SortedArrayList<>(new Comparator<Operation>() {
					@Override
					public int compare(Operation o1, Operation o2) {
						int c = 0;
						WorkOrder wo1 = o1.getWorkOrder();
						WorkOrder wo2 = o2.getWorkOrder();
						if(wo1.getExecutionGroupForcedEnd() != null && wo2.getExecutionGroupForcedEnd() != null){
							c = wo1.getExecutionGroupForcedEnd().compareTo(wo2.getExecutionGroupForcedEnd());
							if(c == 0){
								
								c = o1.getStartDate().compareTo(o2.getStartDate());// preserve current ordering
								/*
								String soh1 = null;
								for(ExecutionGroup g : wo1.getExecutionGroups()){
									if(g.getCode().startsWith("INV_")){
										soh1 = g.getCode();
										break;
									}
								}
								String soh2 = null;
								for(ExecutionGroup g : wo2.getExecutionGroups()){
									if(g.getCode().startsWith("INV_")){
										soh2 = g.getCode();
										break;
									}
								}
								if(soh1 != null && soh2 != null)
									c = soh1.compareTo(soh2);
								*/
							}
						}
						return c;
					}
				});
				
				Resource expe = res.stream().filter(r -> "EXPE".equals(r.getCode())).findFirst().orElse(null);
				
				Set<Operation> expeOps = expe.getOperationList();
				deliverySorted.addAll(expeOps);
				
				//for(Resource r : res)
				{
					/*
					deliverySorted.clear();
					for(Operation o : r.getOperationList()){
						if(o.getNextOperation() == null)
							deliverySorted.add(o);
					}
					*/
					if(deliverySorted.size() < 2)
						continue;
					
					long maxEnd = deliverySorted.get(deliverySorted.size() - 1).getEndDate();
					for(int i = deliverySorted.size() - 1 ; i >= 0; i--){
						Operation o = deliverySorted.get(i);
						Operation next = i < deliverySorted.size() - 1 ? deliverySorted.get(i+1) : null;
						Long e = o.getWorkOrder().getExecutionGroupForcedEnd();
						//System.out.println(e.toString());
						if(e!= null)
							maxEnd = e;
						long startTime = asap(o, maxEnd- o.getDuration(), next, expe);
						if(o.getStartDate() != startTime){
							if(doMove(calc, o, startTime)){
								mod = true;
								updateCounter++;
								dbgCounter++;
							}
						}
						
					}
				}
				
				
				
				System.out.println(iteration + " Initialized "+dbgCounter+" items on this delivery sorted step ");
				
			}while(false);
			
			List<Long> days = new ArrayList<Long>(365);
			sorted.clear();
			sorted.addAll(ops);
			long d = 0;
			for(Operation o : sorted){
				long t = (o.getEndDate()/(24*3600))*24*3600;
				if(t > d){
					d = t;
					days.add(t);
				}
			}
			
			dbgCounter = 0;
			Map<Resource, Long> resourceLoads = new TreeMap<Resource, Long>(new Comparator<Resource>() {
				@Override
				public int compare(Resource o1, Resource o2) {
					int c = o1.getWorkcenter().compareTo(o2.getWorkcenter());
					if(c == 0){
						c = o1.getCode().compareTo(o2.getCode());
						if(c == 0)
							c = o1.getInstance() - o2.getInstance();
					}
					return c;
				}
			});
			
			
			Map<String, List<Resource>> resourceGroups = new HashMap<>();
			//for(Resource r: resourceOperationMap.keySet()){
			for(Resource r: res){
				String key = r.getWorkcenter()+"-"+r.getCode();
				List<Resource> l = resourceGroups.get(key);
				if(l == null){
					l = new ArrayList<Resource>();
					resourceGroups.put(key, l);
				}
				l.add(r);
			}
			
			for(Long day : days){
				resourceLoads.clear();
				Map<Resource, Integer> resourceDayOperationsCount = new HashMap<>();
				//for(Resource r : resourceOperationMap.keySet()){
				for(Resource r : res){
					long load = 0;
					int count = 0;
					//for(Operation o : resourceOperationMap.get(r)){
					for(Operation o : r.getOperationList()){
						long t = (o.getEndDate()/(24*3600))*24*3600;
						if(t == day){
							load += o.getDuration();
							count++;
						}
					}
					resourceLoads.put(r, load);
					resourceDayOperationsCount.put(r, count);
				}
				
				
				
				Resource prev = null;
				long previousLoad = 0;
				for(List<Resource> group : resourceGroups.values()){
					if(group.size() <= 1)
						continue;
					prev = group.get(0);
					previousLoad = resourceLoads.get(prev);
					int previousCount = resourceDayOperationsCount.get(prev);
					for(int i = 1; i < group.size(); i++){
						Resource r = group.get(i);
						//these are equivalent resources
						long load = resourceLoads.get(r);
						int count = resourceDayOperationsCount.get(r);
						//List<Operation> resOps = resourceOperationMap.get(r);
						//if(resOps.size() <= 1)
						//	continue;
						if(load > 1.5*previousLoad && load >= 4*3600 && count > 2){
							//List<Operation> rops = resourceOperationMap.get(r);
							List<Operation> rops = new ArrayList<>(r.getOperationList());
							for(int j = rops.size() - 1; j >= 0; j--){
								Operation o = rops.get(j);
								long t = (o.getEndDate()/(24*3600))*24*3600;
								if(t != day){
									rops.remove(j);
								}
							}
							for(int j = 0; j < rops.size() && load > 1.5*previousLoad ; j++){
								//this one is overloaded
								Operation o = rops.get(j);
								if(o.getDuration() > 0.5*load)
									continue;
								
								long bestRank = 0;
								for(int n = 0; n < rops.size() ; n++){
									if(j == n)
										continue;
									long rk = operationGroupRank(ops, o, rops.get(n));
									if(rk > bestRank)
										bestRank = rk;
								}
								long otherRank = -1;
								List<Operation> other_ops = new ArrayList<>(prev.getOperationList());
								for(int n = 0; n < other_ops.size() ; n++){
									Operation other = other_ops.get(n);
									long t = (other.getEndDate()/(24*3600))*24*3600;
									if(t != day)
										continue;
									long rk = operationGroupRank(ops, o, other);
									if(rk > otherRank){
										otherRank = rk;
										if(otherRank >= bestRank)
											continue;
									}
								}
								
								if(otherRank < 0 || otherRank > bestRank)
								{
									//o = rops.remove(j);
									doMove(calc, o, prev);
									//assert(!resourceOperationMap.get(prev).contains(o));
									//resourceOperationMap.get(prev).add(o);
									load -= o.getDuration();
									previousLoad += o.getDuration();
									resourceLoads.put(r, load);
									resourceLoads.put(prev, previousLoad);
									previousCount++;
									count--;
									resourceDayOperationsCount.put(prev, previousCount);
									resourceDayOperationsCount.put(r, count);
									dbgCounter ++;
									updateCounter++;
									mod = true;		
								}
							}
						}else if(load < 0.5*previousLoad && previousLoad >= 4*3600 && previousCount > 2){
							//List<Operation> rops = resourceOperationMap.get(prev);
							List<Operation> rops = new ArrayList<>(prev.getOperationList());
							for(int j = rops.size() - 1; j >= 0; j--){
								Operation o = rops.get(j);
								long t = (o.getEndDate()/(24*3600))*24*3600;
								if(t != day){
									rops.remove(j);
								}
							}
							
							for(int j = 0; j < rops.size() && load < 0.5*previousLoad ; j++){
								//this one is underloaded
								Operation o = rops.get(j);
								if(o.getDuration() > 0.5*load)
									continue;
								
								long bestRank = 0;
								for(int n = 0; n < rops.size() ; n++){
									if(j == n)
										continue;
									long rk = operationGroupRank(ops, o, rops.get(n));
									if(rk > bestRank)
										bestRank = rk;
								}
								long otherRank = -1;
								List<Operation> other_ops = new ArrayList<>(r.getOperationList());
								for(int n = 0; n < other_ops.size() ; n++){
									Operation other = other_ops.get(n);
									long t = (other.getEndDate()/(24*3600))*24*3600;
									if(t != day)
										continue;
									long rk = operationGroupRank(ops, o, other);
									if(rk > otherRank){
										otherRank = rk;
										if(otherRank >= bestRank)
											continue;
									}
								}
								
								
								if(otherRank < 0 || otherRank > bestRank)
								{
									o = rops.remove(j);
									doMove(calc, o, r);
									//assert(!resourceOperationMap.get(r).contains(o));
									//resourceOperationMap.get(r).add(o);
									load += o.getDuration();
									previousLoad -= o.getDuration();
									resourceLoads.put(r, load);
									resourceLoads.put(prev, previousLoad);
									previousCount--;
									count++;
									resourceDayOperationsCount.put(prev, previousCount);
									resourceDayOperationsCount.put(r, count);
									dbgCounter ++;
									updateCounter++;
									mod = true;
								}
							}
						}
					
						prev = r;
						previousLoad = resourceLoads.get(r);
						//log(r.toString()+" : load "+resourceLoads.get(r));
					}
				}
			}
			resourceLoads.clear();
			resourceLoads = null;
			resourceGroups.clear();
			resourceGroups = null;
			
			//score = calc.calculateScore();
			//System.out.println(iteration + " Initialized "+dbgCounter+" items on this resource balancing step " + score);
			System.out.println(iteration + " Initialized "+dbgCounter+" items on this resource balancing step ");
			
			
			
			
			do{
				mod = false;
				dbgCounter = 0;
				//would actually need to this everytime we change it but...
				
				for(Resource r : res){
					//List<Operation> rops = resourceOperationMap.get(r);
					List<Operation> rops = new ArrayList<>(r.getOperationList());
					sorted.clear();
					sorted.addAll(rops);
				
					for(int i = 0; i < sorted.size(); i++){
						Operation o = sorted.get(i);
						
						long startTime = o.getStartDate();
						if(o.getNextOperation() != null){
							long st = o.getNextOperation().getStartDate() - o.getDuration();
							if(st < startTime)
								startTime = st;
						}
						WorkOrder wo = o.getWorkOrder();
						//if(!mod)
						{
							if(wo.getExecutionGroupForcedStart() != null && wo.getExecutionGroupForcedStart() > startTime){
								
								long s = wo.getExecutionGroupForcedStart();
								long end = o.getOverallEndDate();
								long workingPeriod = (long)((end - s)/(24*3600.0))*8*3600;
								long span = Math.max(5*8*3600, 3*o.getOverallDuration());
								if(workingPeriod > span){	//otherwise it is quite unlikelly
									
									//sorted.clear();
									//sorted.addAll(ops);
									Operation terminal = o;
									while(terminal.getNextOperation() != null)
										terminal = terminal.getNextOperation();
									
									
									if(wo.getExecutionGroupForcedEnd() != null)
										s = wo.getExecutionGroupForcedEnd() - terminal.getDuration();
									else 
										s = s + 3*workingPeriod ;
									s = asap(o, s, i < sorted.size() - 1 ? sorted.get(i+1) : null, r);
									//startTime = wo.getExecutionGroupForcedStart();
									int c = 0; //collapse(calc, sorted, o, s);								
									if(c > 0){
											
										mod = true;
										updateCounter+=c;
										dbgCounter+=c;
										startTime = o.getStartDate();
										
									}
								}
								
							}
						}
						
						if(wo.getExecutionGroupForcedEnd() != null && wo.getExecutionGroupForcedEnd() < startTime + o.getDuration()){
							long st = wo.getExecutionGroupForcedEnd() - o.getDuration();
							if(st < startTime)
								startTime = st;
						}
						
						/*
						if(o.getNextOperation() == null){
							Operation prev = i > 0 ? sorted.get(i-1) : null;
							if(prev != null){
								WorkOrder prevWo = prev.getWorkOrder();
								if(prevWo.getExecutionGroupForcedEnd() != null && prevWo.getExecutionGroupForcedEnd() > wo.getExecutionGroupForcedEnd()){
									long st = prev.getStartDate() - o.getDuration();
									if(st < startTime)
										startTime = st;
								}
										
							}
						}
						*/
						
						//if(o.getStartDate() > startTime || (forcedStart && o.getStartDate() < startTime)){
						if(o.getStartDate() != startTime){
							if(doMove(calc, o, startTime)){
								mod = true;
								updateCounter++;
								dbgCounter++;
								//sorted.remove(o);
								//sorted.add(o);
							}
						}
						
	
					}
				}
				//score = calc.calculateScore();
				//System.out.println(iteration + " Initialized "+dbgCounter+" on a time sequence step " + score);
				System.out.println(iteration + " Initialized "+dbgCounter+" on a time sequence step ");
			
				
			
				dbgCounter = 0;
				//for(Resource r : resourceOperationMap.keySet()){
				for(Resource r : res){
					//List<Operation> rops = resourceOperationMap.get(r);
					List<Operation> rops = new ArrayList<>(r.getOperationList());
					if(rops.size() < 2)
						continue;
					sorted.clear();
					sorted.addAll(rops);
					long maxEnd = Long.MAX_VALUE;// sorted.get(sorted.size() - 1).getStartDate();
					for(int i = sorted.size() - 1; i >= 0; i--){
						
						Operation o = sorted.get(i);
						long bestStart = i < sorted.size() - 1 ? sorted.get(i+1).getStartDate() - o.getDuration() : o.getStartDate();
						if(o.getNextOperation() != null && o.getNextOperation().getStartDate() - o.getDuration() < bestStart)
							bestStart = o.getNextOperation().getStartDate() - o.getDuration();
						long maxStart = asap(sorted, r, i, bestStart);
						if(o.getStartDate() > maxStart){	
						//if(o.getStartDate() != maxStart){
							
							if(i < sorted.size() - 1){
								Operation n = sorted.get(i+1);
								if(maxStart >= n.getStartDate()){
									log("WTF");
								}
							}
							
							if(doMove(calc, o, maxStart)){
								mod = true;
								dbgCounter  ++;
								updateCounter++;
							}
						}
					}
				}
				//score = calc.calculateScore();
				//System.out.println(iteration + " Initialized " + dbgCounter + " on simultaneous operation step " + score);
				System.out.println(iteration + " Initialized " + dbgCounter + " on simultaneous operation step ");
			}while(false);
			
			
			//mod = false;
			dbgCounter = 0;
			//for(Resource r : resourceOperationMap.keySet()){
			for(Resource r : res){
				
				//List<Operation> rops = resourceOperationMap.get(r);
				
				sorted.clear();
				sorted.addAll(r.getOperationList());
				
				for(int i = sorted.size() - 1; i >= 0; i--){
					Operation o = sorted.get(i);
					
					if(!o.isDurationEnabled())
						continue;
					
					WorkOrder wo = o.getWorkOrder();
					long targetStart = -1;
					Operation next = i < sorted.size() - 1 ? sorted.get(i+1) : null;
					
					if(o.getNextOperation() == null){ //prevent unbounded, unless we have a forced end					
						
						long startTime = o.getStartDate();
						if(wo.getExecutionGroupForcedEnd() != null){
							targetStart = wo.getExecutionGroupForcedEnd() - o.getDuration();
							//on the case of terminal ops (EXPE) try to sequence by delivery date
							/*
							if(next != null){
								WorkOrder nextWo = next.getWorkOrder();
								if(nextWo.getExecutionGroupForcedEnd() != null && nextWo.getExecutionGroupForcedEnd() < wo.getExecutionGroupForcedEnd()){
									//try to swap them
									Operation afterNext = i < sorted.size() - 2 ? sorted.get(i+2) : null;
									next = afterNext; 
								}
							}
							*/
							//continue;
						}else{
							continue; // nothing we can do about it... moving on
						}
					}else{
						targetStart = o.getNextOperation().getStartDate() - o.getDuration();
					}
					
					
					Operation originalNext = next;
					long maxStart = targetStart;
					long bestDRank = Long.MIN_VALUE;
					Operation bestNext = null;
					long bestStart = 0;
					
					if(i < sorted.size() - 1){
						//int indexOfOperation = opIndexes.get(o); //TODO NPE
						long deltaRank = 0;
						
						if(i == 0){
							deltaRank = -1*operationGroupRank(ops, o, sorted.get(i+1));
						}else{
							deltaRank = operationGroupRank(ops, sorted.get(i-1), sorted.get(i+1));
							deltaRank -= operationGroupRank(ops, sorted.get(i-1), o);
							deltaRank -= operationGroupRank(ops, o, sorted.get(i+1));
						}
						long forcedEnd = wo.getExecutionGroupForcedEnd() == null ? 0 : wo.getExecutionGroupForcedEnd();
						
						//deltaRank = operationGroupRank(ops, o, sorted.get(i+1));
						
						//need to also evaluate current status so we can adjust to next
						Operation pr = o;
						//double sumOfDurations = 0;
						for(int j = i + 1; j < sorted.size(); j++){
							Operation t = sorted.get(j);
							//sumOfDurations += t.getDuration();
							WorkOrder woT = t.getWorkOrder();
							long forcedEndT = woT.getExecutionGroupForcedEnd() == null ? 0 : woT.getExecutionGroupForcedEnd();
							//using the grouping horizon causes trouble if WO has room in front of it
							//it cannot move ahead because its inside it's grouping horizon
							//so we allow first t and require group horizon after that
							//if((forcedEndT*forcedEnd == 0) || forcedEndT <= forcedEnd)
							{
								if((j == i +1) || ((t.getStartDate() > o.getEndDate() + 1.5*r.getGroupingEOQ()) ))
								{	// minimize impact on grouping
									//if I were to start before t when would it be?
									long s = asap(o, maxStart, t, r);
									//if(s > pr.getEndDate() - Math.abs(mRandom.nextGaussian()/6)*o.getDuration()) //admit some overlapp
									
									
									//boolean allowOverlap = forcedEndT > 0 && forcedEnd > 0 && forcedEndT < forcedEnd;
									if( (s > pr.getEndDate() - 0.0*o.getDuration())) //admit some overlapp
									{
										//int nlrank = operationGroupRank(ops, opIndexes.get(pr), indexOfOperation);
										//int nrrank = operationGroupRank(ops, indexOfOperation, opIndexes.get(t));
										
										//int nBest = nlrank + nrrank; // Math.max(nlrank, nrrank);
										//if(nBest >= currentBestRank)
										
										//to be fair, when i = 0 we can only do a right check
										long dRank = 0;
										
										if(i == 0){
											//dRank = -1*operationGroupRank(ops, pr, t);
											dRank += operationGroupRank(ops, o, t);
										}else{
											dRank = -1*operationGroupRank(ops, pr, t);
											dRank += operationGroupRank(ops, pr, o);
											dRank += operationGroupRank(ops, o, t);
										}
										long delta = dRank + deltaRank;
										
										//if((dRank + deltaRank >= 0) || (s - o.getStartDate() > 10*r.getGroupingHorizon() && j - 1 > 10))
										//if(dRank + deltaRank >= 0)
										
										//dRank = operationGroupRank(ops, o, t);
										//long delta = dRank - deltaRank;
										
										if(delta >= 0) // || (s - o.getStartDate() > 2*r.getGroupingHorizon()))
										{
										//if(nlrank >= lrank && nrrank >= rrank){
											targetStart = s;
											next = t;
											break; //don't get greedy so we can instead try our options
										}else if(delta > bestDRank){
											bestDRank = delta;
											bestNext = t;
											bestStart = s;
										}
									}						
								}
							}
							pr = t;
							if(t.getStartDate() >= maxStart)
								break;
						}
					}
					/*
					if(targetStart >= maxStart){
						//no luck in finding a target start
						if(bestNext != null && acceptMove(o, bestStart)){
							targetStart = bestStart;
							next = bestNext;
						}
						
					}
					*/

					//long maxStart =asap(sorted, r, i, targetStart);
					maxStart = asap(o, targetStart, next, r );
					if(maxStart > o.getStartDate()){//give it a try
						if(acceptMove(o, maxStart))
						{
							doMove(calc, o, maxStart);
							dbgCounter ++;
							updateCounter ++;
							mod = true;
							sorted.remove(o);
							sorted.add(o);
							//sorted.clear();
							//sorted.addAll(rops);//we messed up sorted list
						}
					}else{
						//unable to move any further because we have an operation occupying next slot
						//but if we can jump over next operation maintaining grouping score we may get away with it
//						if(i < sorted.size() - 2 && i > 0){
//							Operation left = sorted.get(i-1);
//							Operation right = sorted.get(i+1);
//							int oIndex = opIndexes.get(o);
//							int lIndex = opIndexes.get(left);
//							int rIndex = opIndexes.get(right);
//							int currentScore = operationGroupRank(ops, lIndex, oIndex);
//							currentScore += operationGroupRank(ops, oIndex, rIndex);
//							
//							
//							left = sorted.get(i+1);
//							right = sorted.get(i+2);
//							lIndex = opIndexes.get(left);
//							rIndex = opIndexes.get(right);
//							int wouldBeScore = operationGroupRank(ops, lIndex, oIndex);
//							wouldBeScore += operationGroupRank(ops, oIndex, rIndex);
//							
//							Operation n = left; //sorted.get(i+1);
//							
//							if(wouldBeScore > currentScore){
//								//looks like it would be a good ideia to swap them
//								//long oldStart = o.getStartDate();
//								targetStart = n.getEndDate() - o.getDuration();
//								maxStart = asap(o, targetStart, right, r);
//								if(maxStart > o.getStartDate()){//give it a try
//									if(acceptMove(o, maxStart)){
//										doMove(calc, o, maxStart);
//										doMove(calc, right, o.getStartDate() - right.getDuration());
//										dbgCounter ++;
//										updateCounter ++;
//										mod = true;
//									}
//								}
//							
//							}
//							
//							
//							
//						}
					}
					
					
					
				}
			}
			//score = calc.calculateScore();
			//System.out.println(iteration + " Initialized " + dbgCounter + " on move ahead step " + score);
			System.out.println(iteration + " Initialized " + dbgCounter + " on move ahead step ");
			
			
			if(!mod)
			{
				
				
				
				mod = false;
				dbgCounter = 0;
				Map<Operation, Integer> operationsBestMatch = new HashMap<>();
				//for(Resource r : resourceOperationMap.keySet()){
				for(Resource r : res){
					
					long GROUPING_RANGE = r.getGroupingHorizon();
					long GROUPING_PERIOD = r.getGroupingEOQ();
					
					//if("EXPE".equals(r.getCode()))
					//	continue;
					
					//if(!"LINPINT".equals(r.getCode()) || "COR".equals(r.getWorkcenter()))
					//	continue;
					
					
					//List<Operation> rops = resourceOperationMap.get(r);
					List<Operation> rops = new ArrayList<>(r.getOperationList());
					
					
					if(rops.size() < 2)
						continue;
					
					sorted.clear();
					sorted.addAll(rops);
					Operation[] sortedArray = new Operation[sorted.size()];
					sorted.toArray(sortedArray);
					
					int sortedIndexes[] = new int[sortedArray.length];
					for(int i = 0; i < sortedArray.length; i++){
						sortedIndexes[i] = opIndexes.get(sortedArray[i]); //ops.indexOf(sortedArray[i]);
					}
					
					//for(int i = 0; i < sortedArray.length ; i++){
					for(int i = sortedArray.length - 1; i >= 0 ; i--){
						Operation o = sortedArray[i];
						
						
						int oIndex = sortedIndexes[i];
						int bestMatch = 0;
						int bestMatchIndex = -1;
						Operation bestOp = null;
						long sumOfDurations = 0;
						boolean eoq_achieved = GROUPING_PERIOD <= 0;
						boolean searching_eoq = GROUPING_PERIOD > 0;
						int skip_i = -1;
						
						if(!o.isDurationEnabled())
							continue;
						
						//for(int j = i + 1; j < sortedArray.length; j++){
						for(int j = i - 1; j >= 0; j--){
							Operation t = sortedArray[j];
							
							if(!t.isDurationEnabled())
								continue;

							//if((t.getStartDate() - o.getEndDate() > GROUPING_RANGE))	//this should magically split groups
							//	break;
							if( (o.getStartDate() - t.getEndDate() > GROUPING_RANGE))	//this should magically split groups
								break;
							
							
							int tIndex = sortedIndexes[j];
							int rank = operationGroupRank(ops, oIndex, tIndex); 
							//if(t.getNextOperation() != null && t.getNextOperation().equals(o))
							//	rank *= 100;//maximize splitted ops sequencing
							if(rank > bestMatch){
								bestMatch = rank;
								bestMatchIndex = j;
								bestOp = t;
							}
							
							if(searching_eoq){
								if((rank / 100000) >= 100){
									sumOfDurations += t.getDuration();
								}else{
									searching_eoq = false;
									if(sumOfDurations >= GROUPING_PERIOD){
										//we have achieved eoq
										skip_i = j;
										break;
									}
								}
							}
							
						}
						
						operationsBestMatch.put(o, bestMatch);
						
						if(!(bestMatchIndex < 0 || bestMatchIndex == i-1)){
							//if(bestMatchIndex < 0 || bestMatchIndex == i - 1)
								//continue;
							
							int bestOpSortedIndex = sortedIndexes[bestMatchIndex];
							//swap them out
							
							/*
							for(int j = bestMatchIndex ; j > i + 1 ; j--){
								sortedArray[j] = sortedArray[j-1];
								sortedIndexes[j] = sortedIndexes[j-1];
							}
							sortedArray[i + 1] = bestOp;
							sortedIndexes[i + 1] = bestOpSortedIndex;
							*/
							
							for(int j = bestMatchIndex ; j < i - 1 ; j++){
								sortedArray[j] = sortedArray[j+1];
								sortedIndexes[j] = sortedIndexes[j+1];
							}
							sortedArray[i - 1] = bestOp;
							sortedIndexes[i - 1] = bestOpSortedIndex;
							
						}
						if(skip_i >= 0){
							i = skip_i;
						}
						
						
					}
					
					Operation o = sortedArray[sortedArray.length - 1];
					long maxEnd = o.getEndDate();
					for(int i = sortedArray.length - 1; i >= 0; i--){
						o = sortedArray[i];
						
						long newStart = asap(sortedArray, r, i, maxEnd - o.getDuration());
						//if(o.getNextOperation() != null && o.getNextOperation().getStartDate() < maxEnd)
						//	maxEnd = o.getNextOperation().getStartDate();
						
						//long newStart = maxEnd-o.getDuration();
						//if(o.getStartDate() > newStart && acceptMove(o, newStart)){
						if(o.getStartDate() > newStart){
							if(i < sortedArray.length - 1){
								Operation n = sortedArray[i+1];
								if(newStart >= n.getStartDate()){
									log("WTF.");
								}
							}
							
							if(doMove(calc, o, newStart)){
								dbgCounter++;
								updateCounter++;
								mod = true;
							}
						}
						maxEnd = o.getStartDate();
					}
				}
				
				//second step, check if compatible resource has a better match
				//if so move the op to that resource
				
				//for(int i = 0; i < ops.size(); i++){
				//	opIndexes.put(ops.get(i), i);
				//}
				for(int i = 0; i < ops.size(); i++){
					Operation o = ops.get(i);
					int bestLocalMatch = 0;
					int bestResourceMatch = 0;
					Resource bestResource = null;
					long GROUPING_RANGE = o.getResource().getGroupingHorizon();
					long intervalS = o.getStartDate() - GROUPING_RANGE;
					long intervalE = o.getEndDate() + GROUPING_RANGE;
					
					for(Resource r : o.getResourceRange()){
						//Set<Operation> rops = r.getOperationList();
						//List<Operation> rops = resourceOperationMap.get(r);
						List<Operation> rops = new ArrayList<>(r.getOperationList());
						sorted.clear();
						sorted.addAll(rops);
						for(int j = sorted.size() - 1; j >= 0; j--){
							Operation ro = sorted.get(j);
							if(!(ro.getStartDate() <= intervalE && ro.getEndDate() >= intervalS))
								continue;
								
							if(!opIndexes.containsKey(ro)){
								System.err.println("Index not found for "+ro.getCode());
								continue;
							}
							
							int rank = operationGroupRank(ops, i, opIndexes.get(ro));
							if(r.equals(o.getResource())){
								if(rank > bestLocalMatch){
									bestLocalMatch = rank;
								}								
							}else if(rank > bestResourceMatch){
								bestResourceMatch = rank;
								bestResource = r;
							}
							
						}
					}
					
					if(bestResource != null && bestResourceMatch > bestLocalMatch){
						//if(acceptMove(o, bestResource))
						{
							//resourceOperationMap.get(o.getResource()).remove(o);
							doMove(calc, o, bestResource);
								
							//assert(!resourceOperationMap.get(o.getResource()).contains(o));
							//resourceOperationMap.get(o.getResource()).add(o);
							dbgCounter++;
							updateCounter++;
							mod = true;	
							
						}
					}
				}
				operationsBestMatch.clear();
				operationsBestMatch = null;
				
				System.out.println(iteration + " Initialized "+dbgCounter+" on grouping step ");

				
				
			}
			
			
			
			
			score = calc.calculateScore();
			System.out.println(iteration + " Current score "+score);
			
			if(mBestScoreSync.tryLock()){
				try{
					//if(mBestScore == null || score.compareTo(mBestScore) > 0 || (updateCounter == 0 && lastUpdateCounter > 0))
					if(mBestScore == null || score.compareTo(mBestScore) > 0)
					{
						System.out.println("New best local score " + score);
						mBestScore = score;
						setBestSchedule(mBestScore, sch);
					}
				}finally {
					mBestScoreSync.unlock();
				}
			}
			
			if(mod){
				//lastModTime = System.currentTimeMillis();
				noModCounter = 0;
			}else{
				noModCounter++;
				long dly = Math.min(10000,  1000*noModCounter);
				
				//turn up the heat for simulated annealing
				double r = 1 - 0.1*(dly / 10000.0);
				for(String code : mOperationTiredness.keySet()){
					long t = mOperationTiredness.get(code);
					mOperationTiredness.put(code, (long)(t*r));
				}
					
					
				try {
					Thread.sleep(dly);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
			}
			
			
			//iteration++;
		}while(mRunning);
		
		
	}
	
	//private int collapse(OperationIncrementalScoreCalculator calc, Operation o) {
	//	return collapse(calc, o, -1);
	//}
	
	
	/**
	 * move this and all next operations to their latest possible time
	 * @param sorted 
	 * @param o
	 */
	private int collapse_(OperationIncrementalScoreCalculator calc, SortedArrayList<Operation> sorted, Operation o, long maxStart) {
		int res = 0;
		Operation n = o.getNextOperation();
		if(n != null){
			res += collapse(calc, sorted, n, maxStart);
			long startTime = n.getStartDate() - o.getDuration();			
			if(startTime - o.getStartDate() > 2*o.getResource().getGroupingHorizon()){ //minimize impact on grouping
				
				if(acceptMove(o, startTime))
				{
					if(doMove(calc, o, startTime, false)){						
						res++;
						sorted.remove(o);
						sorted.add(o);
					}
				}
			}
		}else{
			long startTime = maxStart > 0 ? maxStart : o.getStartDate();	
			if(startTime != o.getStartDate()){
				if(acceptMove(o, startTime))
				{
					if(doMove(calc, o, startTime, false)){
						res++;
						sorted.remove(o);
						sorted.add(o);
					}
				}
			}
		}
		
		return res;
	}
	private int collapse(OperationIncrementalScoreCalculator calc, SortedArrayList<Operation> sorted, Operation o, long maxStart) {
		int res = 0;
		Operation n = o.getNextOperation();
		if(n != null){
			res += collapse(calc, sorted, n, maxStart);
			long startTime = n.getStartDate() - o.getDuration();
			boolean gapFound = false;
			
			Operation nxt = null;
			Operation prv = null;
			
			for(int i = sorted.size() -1 ; i >= 0; i--){
				prv = sorted.get(i);
				if(!prv.getResource().equals(o.getResource()))
					continue;
				if(prv.equals(o))
					break;
				if(nxt != null && (nxt.getStartDate() - o.getStartDate()) > o.getResource().getGroupingHorizon()){ //minimize impact on grouping
				//if(prv.getEndDate() < startTime && nxt != null){
					long gap = nxt.getStartDate() - prv.getEndDate();
					if(gap >= o.getDuration()){
						long asap = asap(o, startTime, nxt, o.getResource());
						if(asap > prv.getEndDate() && acceptMove(o, asap)){
							startTime = asap;
							gapFound = true;
							break;
						}
					}
				}
				nxt = prv;
			}
			
			
			if(gapFound && startTime > o.getStartDate()){
				
				//if(acceptMove(o, startTime))
				{
					if(doMove(calc, o, startTime, false)){						
						res++;
						sorted.remove(o);
						sorted.add(o);
					}
				}
			}
		}else{
			long startTime = maxStart > 0 ? maxStart : o.getStartDate();	
			if(startTime > o.getStartDate()){
				if(acceptMove(o, startTime))
				{
					if(doMove(calc, o, startTime, false)){
						res++;
						sorted.remove(o);
						sorted.add(o);
					}
				}
			}
		}
		
		return res;
	}

	protected long asap(Operation[] sorted, Resource res, int opIndex, long targetStart){
		return asap(sorted[opIndex], targetStart, opIndex < sorted.length - 1 ? sorted[opIndex+1] : null, res);
	}
	protected long asap(List<Operation> sorted, Resource res, int opIndex, long targetStart){
		return asap(sorted.get(opIndex), targetStart, opIndex < sorted.size() - 1 ? sorted.get(opIndex+1) : null, res);
	}
	protected long asap(Operation o, long targetStart, Operation next, Resource res){
		
		long maxEndTime = targetStart + o.getDuration();
		if(o.getNextOperation() != null && o.getNextOperation().getStartDate()  < maxEndTime ){
			maxEndTime = o.getNextOperation().getStartDate();
		}
		if(next != null){
			if(next.getStartDate() < maxEndTime){
				maxEndTime = next.getStartDate();
			}
		}
		
		long hour = (maxEndTime % (24*3600));
		long dayT = ((maxEndTime) /(24*3600))*24*3600;
		
		//assert(endHour < 24*3600);
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		if(hour > 18*3600){
			hour = (long)(18*3600);
		}else if(hour > 12.5*3600 && hour < 14*3600){
			hour = (long) (12.5*3600);
		}else if(hour <= 8.5*3600 || hour > 24*3600){
			hour = (long) (18*3600);
	        cal.setTimeInMillis(dayT*1000);
	        int idx = cal.get(Calendar.DAY_OF_WEEK);	//note that Calendar.SUNDAY = 1
	        if(idx == 2)
	        	dayT -= 3*24*3600;
	        else if(idx == 1)
	        	dayT -= 2*24*3600;
	        else
	        	dayT -= 24*3600;
		}
		
		maxEndTime = dayT + hour;
		
		return maxEndTime - o.getDuration();
		
		
	}
	
	
	private void log(String message){
		System.out.println(Thread.currentThread().getId()+" > " +  message);
	}
	
	
	protected boolean acceptMove(Operation o, Long target){
		if(1*target == o.getStartDate())
			return false;
		if(!o.isMovable())
			return false;
		
	
		/*
		//tireredess takes too long to converge with a lot of OFs
		Long t = mOperationTiredness.getOrDefault(o.getCode(), 0L);
		
		mOperationTirednessMax =  Math.max(mOperationTirednessMax, t);
		if(mOperationTirednessMax > 0 && (10.0*(t/(double)mOperationTirednessMax) < Math.abs(mRandom.nextGaussian())))
		//if((mRandom.nextDouble() * mOperationTirednessMax + 0.5) >= 0.5*t)
			return true;
		
		return false;
		*/
	
		List<Long> os = mLastMoves.get(o);
		if(os == null){
			os = new ArrayList<Long>(30);
			mLastMoves.put(o, os);
		}
//		
//		/*
//		if(os.size() > 0){
//			if(mRandom.nextDouble() < 0.90)
//				os.remove(0);
//		}
//		if(os.size() > 0)
//			return false;
//		os.add(target);
//		return true;
//		*/
//		
		if(os.size() > 0 && (mRandom.nextDouble() > 0.60 || os.size() > 20)){
		//while(os.size() > 30){
			os.remove((int)(mRandom.nextInt(os.size())));
		}
		boolean res = !os.contains(target);
		//by allowing duplicates we make sure the targets most used remain in the list the most time
		//thus disallowing it's usage
		if(res)
			os.add(target);
		return res;
//		//return true;
	}
	
	
	public void doMove(Operation o, long newStart){
		lockWorkingSolution();
		if(mCalculator != null){
			doMove(mCalculator, o,  newStart);
		}else{			
			o.setStartDate(newStart);
			mWorkingSolutionChanged = true;
		}
		releaseWorkingSolution(false);
	}
	
	protected boolean doMove(OperationIncrementalScoreCalculator calc, Operation o, long targetStart){
		return doMove(calc, o, targetStart, true);
	}
		
	protected boolean doMove(OperationIncrementalScoreCalculator calc, Operation o, long targetStart, boolean movePredecessors){	
		
		if(!o.isMovable())
			return false;
		if(targetStart == o.getStartDate())
			return false;
		
		Long t = mOperationTiredness.get(o.getCode());
		if(t == null || t == 0){
			t = mOperationTirednessMax > 1 ? mOperationTirednessMax/2 : 1;
			//t = 1L;
		}else if (t < 1e50){
			t += Math.max(1, mOperationTirednessMax / t);
			//t += 1;
		}
		mOperationTiredness.put(o.getCode(), t);
		//if(1*targetStart > o.getMaxStartDate())
		//	return;

		calc.beforeVariableChanged(o, "startDate");
		o.setStartDate(targetStart);
		calc.afterVariableChanged(o, "startDate");

		//if("0011711MFG00019486.5".equals(o.getCode()) || "0011711MFG00018207.5".equals(o.getCode())){
		//	log(o.toString()+" "+Instant.ofEpochSecond(o.getStartDate()).toString());
		//}
		
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
		/*
		if(movePredecessors){
			for(Operation p : o.getPreviousOperations()){
				if(p.getEndDate() > o.getStartDate()){
					doMove(calc, p, o.getStartDate() - p.getDuration(), true);
				}
			}
		}
		*/
		if(movePredecessors){
			for(Operation p : o.getPreviousOperations()){
				if(p.getEndDate() > o.getStartDate()){
					doMove(calc, p, o.getStartDate() - p.getDuration(), true);
				}
			}
			
			if(o.getNextOperation() != null && o.getNextOperation().getStartDate() < o.getEndDate()){
				doMove(calc, o.getNextOperation(), o.getEndDate());
			}
		}
		
		return true;
		
	}
	
	
	public void doMove(Operation o, Resource newResource){
		lockWorkingSolution();
		if(mCalculator != null){
			doMove(mCalculator, o,  newResource);
		}else{
			o.getResource().getOperationList().remove(o);		
			o.setResource(newResource);
			newResource.getOperationList().add(o);
			mWorkingSolutionChanged = true;
		}
		releaseWorkingSolution(false);
	}

	
	protected boolean doMove(OperationIncrementalScoreCalculator calc, Operation o, Resource res){
		
		//if(!o.isMovable())
		//	return false;
		//if(res.equals(o.getResource()))
		//	return false;
		
		calc.beforeVariableChanged(o, "resource");
		
		o.getResource().getOperationList().remove(o);		
		o.setResource(res);
		assert(!res.getOperationList().contains(o));
		res.getOperationList().add(o);
		calc.afterVariableChanged(o, "resource");
		return true;
	}
	

	private static class CacheKey implements Serializable{

	    /**
		 * 
		 */
		private static final long serialVersionUID = 3581316604543845311L;
		private final String x;
	    private final String y;
	    

	    public CacheKey(String x, String y) {
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
	    	if(x.compareTo(y) < 0)
	    		return new HashCodeBuilder().append(x).append(y).toHashCode();
	    	else
	    		return new HashCodeBuilder().append(y).append(x).toHashCode();
	    }

	}
	
	
	
	
	protected Map<CacheKey, Integer> computeGroupRankCache(final List<Operation> ops, Map<CacheKey, Integer> oldCache, long startTime){
		
		Map<CacheKey, Integer> cache = new HashMap<CacheKey, Integer>();
		//prefill cache with existent and remove from ops
		if(oldCache != null){
			for(int i = 0; i < ops.size(); i++)
			{
				Operation o1 = ops.get(i);
				for(int j = 0; j < ops.size(); j++)
				{
					Operation o2 = ops.get(j);
					CacheKey k = new CacheKey(o1.getCode(), o2.getCode());
					Integer v = oldCache.get(k);
					if(v != null){
						cache.put(k, v);
						oldCache.remove(k); //optimize memory usage and speed
					}
					if(theLastRankComputeTime > startTime)
						return null;	//a new compute task is running;
				}
			}
		}
		
		
		for(int i = 0; i < ops.size(); i++)
		{
			
			if((i % 100) == 0){
				double p = i/(1.0*ops.size());
				System.out.println(String.format("computeGroupRankCache at %.2f %%", p*100));
			}
			
			if(theLastRankComputeTime > startTime)
				return null;	//a new compute task is running;
			
			Operation o1 = ops.get(i);
			List<StockItem> consumedPrev = new ArrayList<StockItem>(10);
			List<StockItem> producedPrev = new ArrayList<StockItem>(10);
			List<String> groupsPrev = new ArrayList<String>();
			WorkOrder wo1 = o1.getWorkOrder();
			
			double resWeight = 1.0;
			if(o1.getResource() != null){
				if("LINPINT".equals(o1.getResource().getCode())){
					resWeight = 2.0;
				}
			}
			
			for(StockItemTransaction tr : wo1.getRequiredTransaction()){
				StockItem it = tr.getItem();
				consumedPrev.add(it);
			}
			for(StockItemTransaction tr : wo1.getProducedTransactionList()){
				producedPrev.add(tr.getItem());
			}
			
			for(ExecutionGroup g : wo1.getExecutionGroups()){
				//reduce the number of match > 0
				switch(g.getCode().substring(0, 3)){
					case "INV":
						groupsPrev.add(g.getCode());
						break;
				}
			}
			
			int bestScore = 0;
			for(int j = i + 1; j < ops.size(); j++)
			{
				
				if(theLastRankComputeTime > startTime)
					return null;	//a new compute task is running;
				
				Operation o2 = ops.get(j);
			
				if(!o1.getResourceRange().contains(o2.getResource()))
					continue;
				
				
				CacheKey cacheKey = new CacheKey(o1.getCode(), o2.getCode());
				//minimize but doesn't solve it because if rank is 0
				//it is not added to hashmap so we have no way to know it was computed
				//already, and saving 0 is not an option
				if(cache.containsKey(cacheKey))
					continue;
				
				WorkOrder wo2 = o2.getWorkOrder();
				int consumedMaterialsScore = 0;
				int producedItemsScore = 0;
				int descriptionMatchScore = 0;
				int groupMatchScore = 0;
				
				for(StockItemTransaction tr : wo2.getRequiredTransaction()){
					StockItem it = tr.getItem();
					if(consumedPrev.contains(it)){
						consumedMaterialsScore += it.getReocod() != 3 ? 100*resWeight : 2;
					}
				}
				
				for(ExecutionGroup g : wo2.getExecutionGroups()){
					if(groupsPrev.contains(g.getCode())){
						groupMatchScore++;
					}
					
				}
				
				
				int minLevenshteinDistance = Integer.MAX_VALUE;
				int maxStrlen = 0;
				for(StockItemTransaction tr : wo2.getProducedTransactionList()){
					StockItem it = tr.getItem();
					if(producedPrev.contains(it)){
						producedItemsScore++;
					}
					String desc = it.getDescription();
					if(desc != null){
						int l = desc.length();
						if(l > 0){
							
							for(StockItem p : producedPrev){
								int bound = Math.max(it.getDescription().length(), p.getDescription().length());
								int d = StringUtils.getLevenshteinDistance(it.getDescription(), p.getDescription(), (int)(0.3*bound));
								if(d >=0 && d < minLevenshteinDistance){
									minLevenshteinDistance = d;
									maxStrlen = bound;
								}
							}
						}
					}
				}
				
				
				if(maxStrlen > 0 && minLevenshteinDistance < Integer.MAX_VALUE){
					double r = 1 - (minLevenshteinDistance / (double)maxStrlen);
					if(r < 0){
						System.err.println("minLevenshteinDistance less than 0");
					}
					r *= 10;
					descriptionMatchScore += r;
				}
				
				int val = descriptionMatchScore + 100*groupMatchScore + 1000*producedItemsScore + 100000*consumedMaterialsScore;
				
				
				if(val > bestScore)
					bestScore = val;
				
				if(val > 0)
					cache.put(cacheKey, val);
				
				//operationGroups[i][j] = (byte)(val > 127 ? 127 : val);
				//operationGroups[j][i] = (byte)(val > 127 ? 127 : val);
			}
			consumedPrev.clear();
			consumedPrev = null;
			producedPrev.clear();
			producedPrev = null;
			
			CacheKey cacheKey = new CacheKey(o1.getCode(), o1.getCode());			
			if(!cache.containsKey(cacheKey) && bestScore > 0)
				cache.put(cacheKey, bestScore);
			//operationGroups[i][i] = (byte)(bestScore > 127 ? 127 : bestScore);
			if((i % 1000) == 0)	//levenshtein distance is memory hungry
				System.gc();
		}
		return cache;
	}
	
	
	protected int operationGroupRank(List<Operation> ops, int a, int b){
		return operationGroupRank(ops, ops.get(a), ops.get(b));
	}
	
	protected int operationGroupRank(List<Operation> ops, Operation a, Operation b){
		
		int v = 0;
		synchronized (groupRankCacheSync) {
			if(groupRankCache != null){
				CacheKey k = new CacheKey(a.getCode(), b.getCode());
				v = groupRankCache.getOrDefault(k, 0);
			}
		}
		
		return v;

	}
	
	
	
	
	private void updateNextOperations(List<Operation> ops , List<WorkOrder> allWo, Map<String, List<WorkOrder>> invoiceMap) {
		
		for(Operation o : ops){
			o.setNextOperation(null);
			o.getPreviousOperations().clear();
		}
		
		
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
		//repeat untill no more matching can be done
		//even if it means putting more than one op pointing to the same transaction
		int iterations = 0;
		do{
			iterations ++;
			usedTrans.clear();
			for(int i = terminalOps.size() - 1; i >= 0; i--){
				Operation o = terminalOps.get(i);
				WorkOrder wo = o.getWorkOrder();
				StockItemProductionTransaction stockProduction = wo.getProducedTransactionList().get(0);
				List<WorkOrder> consumers = new LinkedList<>();
				List<StockItemTransaction> consumerTransaction = new LinkedList<>();
				for(WorkOrder c : invoiceMap.get(stockProduction.getVCR())){
					List<StockItemTransaction> reqs = c.getRequiredTransaction();
					for(StockItemTransaction r : reqs){
						if(usedTrans.contains(r))
							continue;
	
						if(r.getItem().equals(stockProduction.getItem()) && !consumers.contains(c)){
							consumers.add(c);
							consumerTransaction.add(r);
						}
					}
				}
				WorkOrder consumer = null;
				StockItemTransaction consumerTrans = null;
				for(int j = 0; j < consumers.size(); j++){
					WorkOrder c = consumers.get(j);
					StockItemTransaction r = consumerTransaction.get(j);
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
							terminalOps.remove(i);
							break;
						}
					}
				}
				
			}
		}while(usedTrans.size() > 0 && iterations < 100);
		
		
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
				for(int i = wops.size() - 1 ; i >= 0; i--){
					Operation o = wops.get(i);
					o.setNextOperation(null);
				}
				assert(next != null);
				for(int i = wops.size() - 1 ; i >= 0; i--){
					Operation o = wops.get(i);
					o.setNextOperation(next);
					next = o;					
				}
				
			}
			
			
			
			
		}
		
		//commercial BOMs generate false positives
		for(Operation o : ops){
			if(!"EXPE".equals(o.getResource().getCode()) && o.isDurationEnabled()){
				if(o.getNextOperation() == null){
					log("Invalid next operation for "+o.toString());
				}
			}
		}
		
		
		
	}


	public void terminateEarly() {
		mRunning = false;
	}
	
	

	
	
	
	
	

}
