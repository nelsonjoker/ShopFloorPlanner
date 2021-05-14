package pt.sotubo.planner.solver;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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

import pt.sotubo.planner.SortedArrayList;
import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Resource;
import pt.sotubo.planner.domain.Schedule;
import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemProductionTransaction;
import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;

public class NaiveSolver{

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
		void OnNewBestSolution(NaiveSolver solver, Score score);
	}
	public synchronized void addNewBestSolutionListener(NewBestSolutionListener listener){
		mSolutionListeners.add(listener);
	}
	
	
	public NaiveSolver(Schedule workingSolution) {
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
		mMinimumStartTime = ((System.currentTimeMillis()/1000)/(24*3600))*24*3600 - 12*30*24*3600;
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
				
				
				updateNextOperations(ops, allWo, invoiceMap);
				invoiceMap.clear();
				calc.resetWorkingSolution(sch);
				
				
				
				
				res = sch.getResourceList();
				resourceOperationMap = new HashMap<Resource, List<Operation>>();
				for(Resource r : res){
					resourceOperationMap.put(r, new ArrayList<Operation>());
				}
				for(Operation o : ops){
					resourceOperationMap.get(o.getResource()).add(o);
				}
				
				
				mOperationTiredness.clear();
				mOperationTirednessMax = 0;
				groupRankCache = null;
				mWorkingSolutionChanged = false;
				
			}
			
			
			
			iteration++;
			System.out.println("Starting iteration " + iteration + " -------------------------------- ");
			//calc.resetWorkingSolution(mSchedule);
			mod = false;
			
//			dbgCounter = 0;
//			
//			for(WorkOrder w : allWo){
//				Long lastEnd = w.getLastEnd();
//				if(lastEnd == null)
//					continue;
//				for(Operation o : w.getOperations()){
//					long newStart = lastEnd - o.getDuration();
//					if(Math.abs(newStart - o.getStartDate()) > 0.3*o.getDuration() && acceptMove(o, newStart)){
//						doMove(calc, o, newStart);
//						mod = true;
//						dbgCounter ++;
//					}
//					
//				}
//				
//			}
//			score = calc.calculateScore();
//			System.out.println(iteration + " Initialized "+dbgCounter+" items on workorder sync step "+score);
			
			
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
			for(Resource r : resourceOperationMap.keySet()){
				long load = 0;
				for(Operation o : resourceOperationMap.get(r)){
					load += o.getDuration();
				}
				resourceLoads.put(r, load);
			}
			
			Map<String, List<Resource>> resourceGroups = new HashMap<>();
			for(Resource r: resourceLoads.keySet()){
				String key = r.getWorkcenter()+"-"+r.getCode();
				List<Resource> l = resourceGroups.get(key);
				if(l == null){
					l = new ArrayList<Resource>();
					resourceGroups.put(key, l);
				}
				l.add(r);
			}
			
			
			Resource prev = null;
			long previousLoad = 0;
			for(List<Resource> group : resourceGroups.values()){
				if(group.size() <= 1)
					continue;
				prev = group.get(0);
				previousLoad = resourceLoads.get(prev);
				for(int i = 1; i < group.size(); i++){
					Resource r = group.get(i);
					//these are equivalent resources
					long load = resourceLoads.get(r);
					List<Operation> resOps = resourceOperationMap.get(r);
					//if(resOps.size() <= 1)
					//	continue;
					if(load > 1.5*previousLoad){
						List<Operation> rops = resourceOperationMap.get(r);
						for(int j = 0; j < rops.size() && load > 1.5*previousLoad ; j++){
							//this one is overloaded
							Operation o = rops.get(j);
							if(acceptMove(o, prev)){
								o = rops.remove(j);
								doMove(calc, o, prev);
								resourceOperationMap.get(prev).add(o);
								load -= o.getDuration();
								previousLoad += o.getDuration();
								resourceLoads.put(r, load);
								resourceLoads.put(prev, previousLoad);
								dbgCounter ++;
								mod = true;		
							}
						}
					}else if(load < 0.5*previousLoad){
						List<Operation> rops = resourceOperationMap.get(prev);
						for(int j = 0; j < rops.size() && load < 0.5*previousLoad ; j++){
							//this one is underloaded
							Operation o = rops.get(j);
							if(acceptMove(o, r)){
								o = rops.remove(j);
								doMove(calc, o, r);
								resourceOperationMap.get(r).add(o);
								load += o.getDuration();
								previousLoad -= o.getDuration();
								resourceLoads.put(r, load);
								resourceLoads.put(prev, previousLoad);
								dbgCounter ++;
								mod = true;
							}
						}
					}
				
					prev = r;
					previousLoad = resourceLoads.get(r);
					//log(r.toString()+" : load "+resourceLoads.get(r));
				}
			}
			resourceLoads.clear();
			resourceLoads = null;
			resourceGroups.clear();
			resourceGroups = null;
			
			score = calc.calculateScore();
			System.out.println(iteration + " Initialized "+dbgCounter+" items on this resource balancing step " + score);
			
			
			
			dbgCounter = 0;
			dbgResCounter = 0;
			boolean dirForward = iteration % 5 == 0;
			
			
			//simplest approach
			//resort ops in order to have the best match adjacent
			
			
			if(!mod){
				//first step, for each operation in a resource
				//resort them so that matching operations come closer
				Map<Operation, Integer> operationsBestMatch = new HashMap<>();
				for(Resource r : resourceOperationMap.keySet()){
					
					
					if("EXPE".equals(r.getCode()))
						continue;
					
					List<Operation> rops = resourceOperationMap.get(r);
					
					if(rops.size() < 2)
						continue;
					
					sorted.clear();
					sorted.addAll(rops);
					Operation[] sortedArray = new Operation[sorted.size()];
					sorted.toArray(sortedArray);
					
					int sortedIndexes[] = new int[sortedArray.length];
					for(int i = 0; i < sortedArray.length; i++){
						sortedIndexes[i] = ops.indexOf(sortedArray[i]);
					}
					
					//for(int i = 0; i < sortedArray.length ; i++){
					for(int i = sortedArray.length - 1; i >= 0 ; i--){
						Operation o = sortedArray[i];
						int oIndex = sortedIndexes[i];
						int bestMatch = 0;
						int bestMatchIndex = -1;
						Operation bestOp = null;
						
						//for(int j = i + 1; j < sortedArray.length; j++){
						for(int j = i - 1; j >= 0; j--){
							Operation t = sortedArray[j];
							
							
							//if(t.getStartDate() - o.getStartDate() > 2*24*3600)	//this should magically split groups						
							if(o.getStartDate() - t.getEndDate() > 24*3600)	//this should magically split groups
								break;
							
							long maxEnd = t.getNextOperation() == null ? t.getEndDate() - 1 : t.getNextOperation().getStartDate();
							if(t.getEndDate() >= maxEnd)
								continue;						
							
							int tIndex = sortedIndexes[j];
							int rank = operationGroupRank(ops, oIndex, tIndex); 
							if(rank > bestMatch){
								bestMatch = rank;
								bestMatchIndex = j;
								bestOp = t;
							}
						}
						//if(bestMatchIndex < 0 || bestMatchIndex == i+1)
						if(bestMatchIndex < 0 || bestMatchIndex == i - 1)
							continue;
						
						operationsBestMatch.put(o, bestMatch);
						
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
					
					boolean forcedGrouping = mRandom.nextDouble() < 0.05;
					
					Operation o = sortedArray[sortedArray.length - 1];
					long maxEnd = o.getStartDate();
					for(int i = sortedArray.length - 2; i >= 0; i--){ 
						o = sortedArray[i];
						if(o.getEndDate() > maxEnd && ( forcedGrouping || acceptMove(o, maxEnd - o.getDuration()))){
						//if(Math.abs(o.getEndDate() - maxEnd) > 60 && ( forcedGrouping || acceptMove(o, maxEnd - o.getDuration()))){
							doMove(calc, o, maxEnd - o.getDuration());
							dbgCounter++;
							mod = true;
						}
						maxEnd = o.getStartDate();
					}
					
					
					
					
				}
				
				
				//second step, check if compatible resource has a better match
				//if so move the op to that resource
				for(int i = 0; i < ops.size(); i++){
					Operation o = ops.get(i);
					int bestLocalMatch = (int) (1.5*operationsBestMatch.getOrDefault(o, 0));
					long s = o.getStartDate();
					Resource bestResource = null;
					for(Resource r : o.getResourceRange()){
						if(r.equals(o.getResource()))
							continue;
						Set<Operation> rops = r.getOperationList();
						sorted.clear();
						sorted.addAll(rops);
						for(int j = sorted.size() - 1; j >= 0; j--){
							Operation ro = sorted.get(j);
							if(ro.getStartDate() < s - 4*3600)
								continue;
							if(ro.getEndDate() > s + 4*3600)
								continue;
							int rank = operationGroupRank(ops, i, ops.indexOf(ro));
							if(rank > bestLocalMatch){
								bestLocalMatch = rank;
								bestResource = r;
							}
							
						}
					}
					
					if(bestResource != null){
						if(acceptMove(o, bestResource)){
							resourceOperationMap.get(o.getResource()).remove(o);
							doMove(calc, o, bestResource);
							resourceOperationMap.get(bestResource).add(o);
							dbgResCounter ++;
							mod = true;	
						}
					}
				}
				operationsBestMatch.clear();
				operationsBestMatch = null;
				
				score = calc.calculateScore();
				System.out.println(iteration + " Initialized "+dbgCounter+" / " + dbgResCounter + " items on this consumption matching pass " + score);
			}
			
			
			
			dbgCounter = 0;
			
			//crazy attemp
			
			for(WorkOrder wo : allWo){
				for(Operation o : wo.getOperations()){
					if(o.getNEXOPENUM() == 0)
						continue;
					Operation next = null;
					for(Operation n : wo.getOperations()){
						if(n.getOPENUM() == o.getNEXOPENUM()){
							next = n;
							break;
						}
					}
					if(next == null)
						continue;
					long newStart = next.getStartDate() - o.getDuration();
					if(o.getStartDate() > newStart) { // && acceptMove(o, newStart)){
					//if( (Math.abs(newStart - o.getStartDate()) > 0.3*o.getDuration()) && acceptMove(o, newStart)){
						doMove(calc, o, newStart);
						dbgCounter++;
						mod = true;
					}
				}	
			}
			
			score = calc.calculateScore();
			System.out.println(iteration + " Initialized " + dbgCounter + " items on this operation sequencing " + score);
			
//			for(Resource r : resourceOperationMap.keySet()){
//				List<Operation> rops = resourceOperationMap.get(r);
//				if(rops.size() < 2)
//					continue;
//				
//				
//				sorted.clear();
//				sorted.addAll(rops);
//				/*
//				Operation first = sorted.get(sorted.size()-1);
//				long s = first.getStartDate() + 1;
//				if(acceptMove(first, s)){
//					doMove(calc, first, s);
//					dbgCounter++;
//				}
//				*/
//				for(int i = sorted.size() - 2; i >= 0 ; i--){
//					Operation o = sorted.get(i);
//					Operation n = sorted.get(i+1);
//					Operation nextOp = o.getNextOperation();
//					long newStart = n.getStartDate() - o.getDuration();
//					if(nextOp != null){
//						newStart = Math.max(newStart, nextOp.getStartDate() - o.getDuration());
//					}else{
//						Resource dbgRes = o.getResource();
//						if(!"EXPE".equals(dbgRes.getCode())){
//							log(o.toString());
//						}
//					}
//					if( (Math.abs(newStart - o.getStartDate()) > 0.3*o.getDuration()) && acceptMove(o, newStart)){
//					//if( o.getStartDate() < newStart && acceptMove(o, newStart)){
//						doMove(calc, o, newStart);
//						dbgCounter++;
//						mod = true;//TODO: this will never converge to 0
//					}
//				}
//				
//				
//			}
			
			
			dbgCounter = 0;
			int resIdx = -1;
			for(Resource r : res){
				resIdx++;
				List<Operation> rops = resourceOperationMap.get(r);
				if(rops.size() < 2)
					continue;
				//set all ops to non overlapping 5 h per day max
				sorted.clear();
				sorted.addAll(rops);
				long maxEnd = 0;
				for(Operation o : rops){
					Long end = o.getWorkOrder().getExecutionGroupForcedEnd();
					if(end != null && end > maxEnd)
						maxEnd = end;
				}
				if(maxEnd == 0)
					maxEnd = sorted.get(sorted.size() - 1).getEndDate();
				long dayT = (maxEnd /(24*3600)) * 24*3600;
				long hour = maxEnd - dayT;
				long newStart = maxEnd;
				for(int i = sorted.size() - 1; i >= 0 ; i--){
					Operation o = sorted.get(i);
					newStart -= o.getDuration();
					boolean lowerBound = false;
					WorkOrder wo = o.getWorkOrder();
					double minAcceptProbability = 0.3; 
					
					if(wo.getExecutionGroupForcedEnd() != null && newStart + o.getDuration() > wo.getExecutionGroupForcedEnd()){
						newStart = wo.getExecutionGroupForcedEnd() - o.getDuration();
						minAcceptProbability = 1.0;
					}
					
					else if(wo.getExecutionGroupForcedStart() != null && newStart < wo.getExecutionGroupForcedStart()){
						//check if it is possible...
						if(wo.getExecutionGroupForcedEnd() != null && wo.getExecutionGroupForcedEnd() - wo.getExecutionGroupForcedStart() > o.getDuration()){
							if(newStart < wo.getExecutionGroupForcedStart()){
								newStart = (wo.getExecutionGroupForcedStart()/(24*3600))*24*3600 + 18*3600;
								lowerBound = true;
								minAcceptProbability = 1.0;
							}
						}
					}
					
					//long newStart = dayT + hour - o.getDuration();
					hour = (newStart % (24*3600));
					dayT = (Math.max(getMinimumStartTime(), newStart) /(24*3600))*24*3600;
					long endHour = hour + o.getDuration();
					
					//assert(endHour < 24*3600);
					
					if(endHour > 18*3600){
						endHour = (long)(18*3600);
						hour = (long)(endHour - o.getDuration());							
					//}else if(hour < 14*3600 && hour > 12.5*3600){
					}else if(endHour > 12.5*3600 && hour < 14*3600){
						endHour = (long) (12.5*3600);
						hour = (long)(endHour - o.getDuration());
					}else if(hour <= 8.5*3600 || endHour > 24*3600){
						endHour = (long) (18*3600);
						hour = (long)(endHour - o.getDuration());
				        cal.setTimeInMillis(dayT*1000);
				        int idx = cal.get(Calendar.DAY_OF_WEEK);	//note that Calendar.SUNDAY = 1
				        if(idx == 2)
				        	dayT -= 3*24*3600;
				        else if(idx == 1)
				        	dayT -= 2*24*3600;
				        else
				        	dayT -= 24*3600;
					}
					
					newStart = dayT + hour;
					
					if( (newStart < 1*o.getStartDate() || (lowerBound && o.getStartDate() < newStart)) && ( mRandom.nextDouble() <= minAcceptProbability || acceptMove(o, newStart)) ){
					//if( (newStart < 1*o.getStartDate() || (lowerBound && o.getStartDate() < newStart) || Math.abs(newStart - o.getStartDate()) > 3600) && ( mRandom.nextDouble() <= minAcceptProbability || acceptMove(o, newStart)) ){
					//if(  && (newStart < 1*o.getStartDate() || Math.abs(newStart - o.getStartDate()) > 0.2*o.getDuration() )){
						doMove(calc, o, newStart);
						mod = true;
						dbgCounter ++;
					}
					newStart = o.getStartDate();
					
				}
			}
			score = calc.calculateScore();
			System.out.println(iteration + " Initialized "+dbgCounter+" items on this capacity normalization pass " + score);
			
			dbgCounter = 0;
			List<WorkOrder> notHandledWos = new SortedArrayList<>(new Comparator<WorkOrder>(){

				@Override
				public int compare(WorkOrder w1, WorkOrder w2) {
					return w1.getFirstStart().compareTo(w2.getFirstStart());
				}
				
			});
			//notHandledWos.addAll(allWo);
	
			
			for(Operation o : ops){
				if(o.getNextOperation() == null)
					continue;
				long newStart = o.getNextOperation().getStartDate() - o.getDuration();
				if(o.getStartDate() > newStart) { // && acceptMove(o, newStart)){
					doMove(calc, o, newStart);
					dbgCounter++;
					mod = true;
				}
				
				else if( newStart - o.getEndDate() > 3600*Math.abs(mRandom.nextGaussian()) ){
					if(acceptMove(o, newStart)){
						doMove(calc, o, newStart);
						dbgCounter++;
						mod = true;
					}
				}
				
				
			}
			
			
			
			
			Map<StockItem, StockItemCapacityTracker> mProducedItemsStock = new HashMap<>();
			//first step get to know produced items
			//for(int i = notHandledWos.size() - 1; i >= 0; i--){
			for(int i = 0; i < notHandledWos.size(); i++){
				WorkOrder wo = notHandledWos.get(i);
				StockItemTransaction trans = wo.getProducedTransactionList().get(0); 
				StockItem item = trans.getItem();
				StockItemCapacityTracker tr = mProducedItemsStock.get(item);
				if(tr == null){
					tr = new StockItemCapacityTracker(item, 0);
					mProducedItemsStock.put(item, tr);
				}
				tr.insertProduction(trans, wo);
			}
			//based on the items recorded we now setup consumptions
			//for(int i = notHandledWos.size() - 1; i >= 0; i--){
			for(int i = 0; i < notHandledWos.size(); i++){
				WorkOrder wo = notHandledWos.get(i);
				//notHandledWos.remove(i);
				for(StockItemProductionTransaction pr : wo.getProducedTransactionList()){
					StockItemCapacityTracker prtr = mProducedItemsStock.get(pr.getItem());
					prtr.retractProduction(pr, wo);
				}
				
				long t = -1;
				List<StockItemTransaction> requirements = wo.getRequiredTransaction();
				for(StockItemTransaction trans : requirements){
					StockItem item = trans.getItem();
					StockItemCapacityTracker tr = mProducedItemsStock.get(item);
					if(tr != null){
						//indeed this is a produced item
						tr.insertRequirement(trans, wo);
						if(tr.getHardScore() < 0){
							tr.retractRequirement(trans, wo);
							long nxt = tr.nextAvailabilityDate(wo.getFirstStart(), trans.getQuantity());
							if(nxt > t)
								t = nxt;
							tr.insertRequirement(trans, wo);
						}
					}
				}
				
				if(t > 0 && t > wo.getFirstStart()){
					
					for(StockItemTransaction tr : wo.getRequiredTransaction()){
						StockItemCapacityTracker req = mProducedItemsStock.get(tr.getItem());
						if(req != null)
							req.retractRequirement(tr, wo);
					}
					
					
					//long offset = t - wo.getFirstStart();
					//assert(offset > 0);
					long startDate = t;
					sorted.clear();
					sorted.addAll(wo.getOperations());
					for(int j = 0; j < sorted.size(); j++ ){
						Operation o = sorted.get(j);
						//long startDate = o.getStartDate() + offset;
						if(o.isMovable() && o.getStartDate() < startDate){
							//if(acceptMove(o, startDate))
							{
								doMove(calc, o, startDate);
								dbgCounter++;								
								mod = true;
							}
						}
						startDate = o.getEndDate();
					}
					
					for(StockItemTransaction tr : wo.getRequiredTransaction()){
						StockItemCapacityTracker req = mProducedItemsStock.get(tr.getItem());
						if(req != null)
							req.insertRequirement(tr, wo);
					}
					
				}
				
				
				
				for(StockItemProductionTransaction pr : wo.getProducedTransactionList()){
					StockItemCapacityTracker prtr = mProducedItemsStock.get(pr.getItem());
					prtr.insertProduction(pr, wo);
				}
				
			}
			
//			//make another effort to produce as late as possible
			for(int i = 0; i < notHandledWos.size(); i++){
				WorkOrder wo = notHandledWos.get(i); 
				//if(wo.getExecutionGroupForcedEnd() != null)
				//	continue;
				StockItemTransaction trans = wo.getProducedTransactionList().get(0); 
				StockItem item = trans.getItem();
				StockItemCapacityTracker tr = mProducedItemsStock.get(item);
				tr.retractProduction(trans, wo);
				//if(tr.getHardScore() >= 0) //this would mean the itm is irrelevant and not consider the lead time because the score would allways be negative
				{
					
					long t = tr.nextUnAvailabilityDate(wo.getLastEnd(), -0.00001);//if of is unneeded at least we push it to 0 stock boundary
					
					if(t > 0 && wo.getLastEnd() < t - 4*3600){
						long endDate = t;
						sorted.clear();
						sorted.addAll(wo.getOperations());
						//for(int j = sorted.size() - 1 ; j>= 0; j--)
						{
							Operation o = sorted.get(sorted.size() - 1);
							if(o.isMovable() && o.getEndDate() < endDate){
								if(acceptMove(o, endDate - o.getDuration()))
								{
									doMove(calc, o, endDate - o.getDuration());
									dbgCounter++;								
									mod = true;
								}
							}
							//endDate = o.getStartDate();
						}
					}
				}
				tr.insertProduction(trans, wo);
			}
			
			
			mProducedItemsStock.clear();
			mProducedItemsStock = null;
			notHandledWos.clear();
			notHandledWos = null;
			
			
			
			score = calc.calculateScore();
			System.out.println(iteration + " Initialized "+dbgCounter+" items on this stock normalization pass " + score);
			
			
			
			
			
			
			
			//score = calc.calculateScore();
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
			
			if(iteration % 100 == 0){
				log("Reset incremental calculator");
				calc.resetWorkingSolution(sch);
				Score resetScore = calc.calculateScore();
				if(score.compareTo(resetScore) != 0){
					log("Score corruption detected " + score + " != " + resetScore);
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
		
		return acceptMove(o, o.getStartDate() + 1);
//		
//		List<Resource> os = lastResourceMoves.get(o);
//		if(os == null){
//			os = new ArrayList<Resource>(10);
//			lastResourceMoves.put(o, os);
//		}
//		int count = 0;
//		for(Resource r: os){
//			if(r.equals(target))
//				count++;
//		}
//		os.add(target);
//		if(os.size() > 10){
//			os.remove(mRandom.nextInt(os.size()));
//		}
//		
//		return count < 5;
//		//return true;
		
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
		
		for(Operation p : o.getPreviousOperations()){
			if(p.getEndDate() > o.getStartDate()){
				doMove(calc, p, o.getStartDate() - p.getDuration());
			}
		}
		if(o.getNextOperation() != null && o.getNextOperation().getStartDate() < o.getEndDate()){
			doMove(calc, o.getNextOperation(), o.getEndDate());
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
	
	

}
