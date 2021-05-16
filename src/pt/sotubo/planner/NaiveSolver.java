package com.joker.planner;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.optaplanner.core.api.score.Score;
import org.optaplanner.core.api.score.buildin.bendablelong.BendableLongScore;
import org.optaplanner.core.impl.score.director.ScoreDirector;

import com.joker.planner.domain.Operation;
import com.joker.planner.domain.Resource;
import com.joker.planner.domain.Schedule;
import com.joker.planner.domain.StockItem;
import com.joker.planner.domain.StockItemProductionTransaction;
import com.joker.planner.domain.StockItemTransaction;
import com.joker.planner.domain.WorkOrder;
import com.joker.planner.solver.OperationIncrementalScoreCalculator;
import com.joker.planner.solver.ScheduleSolutionInitializer;

public class NaiveSolver {

	public static final int THREAD_COUNT = 1;
	
	
	private Lock mBestScoreSync;
	private Score mBestScore;
	private Schedule mBestSchedule;
	private ArrayList<Score> mTopScores;
	private ArrayBlockingQueue<Move> mGoodMoves;
	private List<NewBestSolutionListener> mSolutionListeners;
	
	
	protected void setBestSchedule(Score score, Schedule sch){
		mBestScoreSync.lock();
		try{
			mBestScore = score;
			mBestSchedule = sch;
			mBestSchedule.setScore((BendableLongScore) score.multiply(1.0));
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
	
	
	public NaiveSolver() {
		mBestScore = null;
		mBestSchedule = null;
		mBestScoreSync = new ReentrantLock();
		
		mGoodMoves = new ArrayBlockingQueue<>(100);
		mSolutionListeners = new ArrayList<>();
		
		/*
		mTopSchedule = sch;
		mSchedule = sch;
		mTopScheduleSync = new Object();
		mTopScoresLock = new ReentrantLock();
		*/
		mTopScores = new ArrayList<Score>(); 
				
	}

	public void run(Schedule sch) {
		/*
		List<WorkOrder> wos = sch.getWorkOrderList();
		for(WorkOrder wo : wos){
			Long firstStart = Long.MAX_VALUE;
			Long lastEnd = Long.MIN_VALUE;
			for(Operation o : wo.getOperations()){
				if(o.getStartDate() < firstStart)
					firstStart = o.getStartDate();
				if(o.getEndDate() > lastEnd)
					lastEnd = o.getEndDate();
			}
			if(firstStart < Long.MAX_VALUE)
				wo.setFirstStart(firstStart);
			if(lastEnd > Long.MIN_VALUE)
				wo.setLastEnd(lastEnd);
			
		}
		*/
		int threadCount = THREAD_COUNT;
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		List<Optimizer> optimizers = new ArrayList<>(threadCount);
		for(int i= 0; i < threadCount; i++){
			System.out.print('-');
		}
		System.out.println();
		for(int i= 0; i < threadCount; i++){
			Optimizer opt = new Optimizer(sch.planningClone());
			optimizers.add(opt);
			System.out.print('.');
		}
		System.out.println();
		for(int i= 0; i < threadCount; i++){
			executor.execute(optimizers.get(i));
		}
		
		try {
			executor.awaitTermination(15, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		/*
		while(true){
			
			Optimizer opt;
			mTopScoresLock.lock();
			opt = new Optimizer(mTopSchedule.planningClone());
			mTopScoresLock.unlock();
			executor.execute(opt);
			
		}
		*/
		
		
		
		
		
	}
	
	
	private class SimulatedScoreCalculator extends OperationIncrementalScoreCalculator{
		
		
		@Override
		public void beforeEntityAdded(Object entity) {
		}
		@Override
		public void beforeVariableChanged(Object entity, String variableName) {
		}
		@Override
		public void afterEntityAdded(Object entity) {
		}
		@Override
		public void afterVariableChanged(Object entity, String variableName) {
		}
		@Override
		public void afterEntityRemoved(Object entity) {
		}
		
	}
	
	private class Move{
		private Move(Move copy){
			this.ThreadId = copy.ThreadId;
			this.Rank = copy.Rank;
			this.Operation = copy.Operation;
			this.OperationIndex = copy.OperationIndex;
			this.StartTime = copy.StartTime;
			this.FirstStart = copy.FirstStart;
			this.LastEnd = copy.LastEnd;
			this.Resource = copy.Resource;
			this.ResourceIndex = copy.ResourceIndex;
			this.Rollback = copy.Rollback;
			
		}
		public Move(Operation o, int index, int rank) {
			this.ThreadId = Thread.currentThread().getId();
			this.Rank = rank;
			this.Operation = o;
			this.OperationIndex = index;
			this.StartTime = o.getStartDate();
			WorkOrder wo = o.getWorkOrder();
			this.FirstStart = wo.getFirstStart();
			this.LastEnd = wo.getLastEnd();
			this.Resource = o.getResource();
			this.ResourceIndex = o.getResourceRange().indexOf(this.Resource);
			
			this.Rollback = new Move(this);
			this.Rollback.Rollback = null;
		}
		public int Rank;
		public long ThreadId;
		public Operation Operation;
		public int OperationIndex;
		private long StartTime;
		public Long FirstStart;
		public Long LastEnd;
		public Resource Resource;
		private int ResourceIndex;
		
		public Move Rollback;
		
		public void setStartTime(long t){
			this.StartTime = t;
			long et = t + Operation.getDuration();
			if(t <= FirstStart){
				FirstStart = t;
				return;
			}
			if(et >= LastEnd){
				LastEnd = et;
				return;
			}
			long min = Long.MAX_VALUE;
			long max = Long.MIN_VALUE;
			for(Operation o : Operation.getWorkOrder().getOperations()){
				if(o.getStartDate() < min)
					min = o.getStartDate();
				if(o.getEndDate() > max)
					max = o.getEndDate();
			}
			if(min < Long.MIN_VALUE)
				FirstStart = min;
			if(max > Long.MIN_VALUE)
				LastEnd = max;
			
		}
		
	}
	
	
	
	private class Optimizer implements Runnable{

		private Schedule mSchedule;
		
		private boolean mRunning;
		private int mRanking;
		private Random mRandom;
		private Map<Operation, List<Long>> lastMoves;
		private Map<Operation, List<Resource>> lastResourceMoves;
		
		private class Momentum{
			int OperationIndex;
			double Gradient;
		}
		
		public Optimizer(Schedule sch){
			mSchedule = sch;
			mRanking = 0;
			lastMoves = new HashMap<Operation, List<Long>>();
			lastResourceMoves = new HashMap<Operation, List<Resource>>();
			mRandom = new Random(System.currentTimeMillis());
		}
		
		private void log(String message){
			System.out.println(Thread.currentThread().getId()+" > " +  message);
		}
		
		
		protected boolean acceptMove(Operation o, Long target){
			if(1*target == o.getStartDate())
				return false;
			List<Long> os = lastMoves.get(o);
			if(os == null){
				os = new ArrayList<Long>(1000);
				lastMoves.put(o, os);
			}
			
			//if(os.size() > 0 && (mRandom.nextDouble() > 0.95 || os.size() > 10)){
			while(os.size() > 30){
				os.remove(mRandom.nextInt(os.size()));
			}
			boolean res = !os.contains(target);
			//by allowing duplicates we make sure the targets most used remain in the list the most time
			//thus disallowing it's usage
			//if(res)
				os.add(target);
			return res;
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
			if(min < Long.MIN_VALUE || max > Long.MIN_VALUE){
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
			o.setResource(res);
			calc.afterVariableChanged(o, "resource");
		}
		
		protected Map<Operation, List<Operation>> operationGroups_(List<Operation> ops){
			
			HashMap<Operation, List<Operation>> operationGroups = new HashMap<Operation, List<Operation>>(ops.size());
			
			for(int i = 0; i < ops.size(); i++){
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
				
				ArrayList<Operation> bestMatches = new ArrayList<Operation>();
				operationGroups.put(o1, bestMatches);
				int bestVal = 0;
				
				for(int j = 0; j < ops.size(); j++){
					if(i == j)
						continue;
					Operation o2 = ops.get(j);
				
					if(!o1.getResourceRange().contains(o2.getResource()))
						continue;
					
					wo = o2.getWorkOrder();
					int val = 0;
					for(StockItemTransaction tr : wo.getRequiredTransaction()){
						StockItem it = tr.getItem();
						if(consumedPrev.contains(it)){
							val += it.getReocod() != 3 ? 10 : 2;
						}
					}
					/*
					for(StockItemTransaction tr : wo.getProducedTransactionList()){
						StockItem it = tr.getItem();
						if(producedPrev.contains(it)){
							val++;
						}
					}
					*/
					if(val >= bestVal){
						if(val > bestVal){
							bestMatches.clear();
							bestVal = val;
						}
						bestMatches.add(o2);
					}
				}
				
				bestMatches.trimToSize();
			}
			
			return operationGroups;
			
			
		}
		
		protected int[][] operationGroups(List<Operation> ops){
			
			int size = ops.size();
			int[][] operationGroups = new int[size][size];
			
			for(int i = 0; i < ops.size(); i++){
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
				for(int j = i + 1; j < ops.size(); j++){
					Operation o2 = ops.get(j);
				
					if(!o1.getResourceRange().contains(o2.getResource()))
						continue;
					
					wo = o2.getWorkOrder();
					int val = 0;
					for(StockItemTransaction tr : wo.getRequiredTransaction()){
						StockItem it = tr.getItem();
						if(consumedPrev.contains(it)){
							val += it.getReocod() != 3 ? 10 : 2;
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
					operationGroups[i][j] = val;
					operationGroups[j][i] = val;
				}
				operationGroups[i][i] = bestScore;
			}
			
			return operationGroups;
			
			
		}
		
		
		@Override
		public void run() {
			Schedule sch =  mSchedule;
			OperationIncrementalScoreCalculator calc = new  OperationIncrementalScoreCalculator();
			calc.resetWorkingSolution(mSchedule);
			
			List<Operation> ops = sch.getOperationList();
			Map<String, List<WorkOrder>> invoiceMap = new HashMap<String, List<WorkOrder>>();
			List<WorkOrder> allWo = sch.getWorkOrderList();
			for(WorkOrder wo  : allWo){
				for (StockItemProductionTransaction stockProduction : wo.getProducedTransactionList()) {
					List<WorkOrder> woList = invoiceMap.get(stockProduction.getVCRNUMORI());
					if(woList == null){
						woList = new LinkedList<>();
						invoiceMap.put(stockProduction.getVCRNUMORI(), woList);
					}
					woList.add(wo);
				}
			}
			
			updateNextWorkOrders(invoiceMap);
			
			List<Resource> res = sch.getResourceList();
			Map<Resource, List<Operation>> resourceOperationMap = new HashMap<Resource, List<Operation>>();
			for(Resource r : res){
				resourceOperationMap.put(r, new ArrayList<Operation>());
			}
			for(Operation o : ops){
				resourceOperationMap.get(o.getResource()).add(o);
			}
			
			//Map<Operation, List<Operation>> operationGroups = operationGroups(ops);
			int operationGroups[][] = operationGroups(ops);
			
			
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
	    	
	    	Score bestLocalScore = null;
	    	Score score = null;
			do{
				
				System.out.println("Starting iteration " + iteration + " -------------------------------- ");
				//calc.resetWorkingSolution(mSchedule);
				mod = false;
				
				dbgCounter = 0;
				for(WorkOrder w : allWo){
					Long lastEnd = w.getLastEnd();
					if(lastEnd == null)
						continue;
					for(Operation o : w.getOperations()){
						long newStart = lastEnd - o.getDuration();
						if(Math.abs(newStart - o.getStartDate()) > 0.3*o.getDuration() && acceptMove(o, newStart)){
							doMove(calc, o, newStart);
							mod = true;
							dbgCounter ++;
						}
						
					}
					
				}
				score = calc.calculateScore();
				System.out.println(iteration + " Initialized "+dbgCounter+" items on workorder sync step "+score);
				
				
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
				
				Resource prev = null;
				long previousLoad = 0;
				for(Resource r: resourceLoads.keySet()){
					if(prev != null && r.getWorkcenter().equals(prev.getWorkcenter()) && r.getCode().equals(prev.getCode())){
						//these are equivalent resources
						long load = resourceLoads.get(r);
						if(load > 1.1*previousLoad){
							while(load > 1.1*previousLoad){
								//this one is overloaded
								Operation o = resourceOperationMap.get(r).remove(0);
								doMove(calc, o, prev);
								resourceOperationMap.get(prev).add(o);
								load -= o.getDuration();
								previousLoad += o.getDuration();
								resourceLoads.put(r, load);
								resourceLoads.put(prev, previousLoad);
								dbgCounter ++;
								mod = true;							
							}
						}else if(load < 0.9*previousLoad){
							while(load < 0.9*previousLoad){
								//this one is overloaded
								Operation o = resourceOperationMap.get(prev).remove(0);
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
				resourceLoads.clear();
				resourceLoads = null;
				
				score = calc.calculateScore();
				System.out.println(iteration + " Initialized "+dbgCounter+" items on this resource balancing step " + score);
				
				
				
				
				dbgCounter = 0;
				dbgResCounter = 0;
				boolean dirForward = iteration % 2 == 0;
				sorted.clear();
				//sorted.addAll(ops);
				List<Operation> moveForward = new ArrayList<Operation>(ops.size()/2);
//				for(Operation o : ops){
//					WorkOrder next = o.getWorkOrder().getNextOrder();
//					if(next != null && next.getOperations() != null){
//						for(Operation n : next.getOperations()){
//							if(n.getStartDate() < o.getEndDate() && !moveForward.contains(n)){
//								moveForward.add(n);
//							}
//						}
//					}
//				}
				sorted.clear();
				sorted.addAll(ops);
				int sortedIndexes[] = new int[sorted.size()];
				for(int i = 0; i < sorted.size(); i++){
					sortedIndexes[i] = ops.indexOf(sorted.get(i));
				}
				for(int i = 0; i < sorted.size(); i++){
					if(mRandom.nextDouble() > 0.8)
						continue;
				
					Operation o = sorted.get(i);
					/*
					if(!"PIN".equals(o.getResource().getWorkcenter())){
						if(mRandom.nextDouble() > 0.2)
							continue;
					}
					*/
					Operation bestOp = null;
					int offset = 0;
					int bestVal = 0;
					int oIndex = sortedIndexes[i];// ops.indexOf(o);
					int bestPossible = operationGroups[oIndex][oIndex];
					/*
					for(int j = 0; j < sorted.size(); j++){
						if(j == i)
							continue;
						Operation t = sorted.get(j);
						if(!o.getResource().equals(t.getResource()))
							continue;
						int tIndex = sortedIndexes[j];
						bestPossible = Math.max(bestPossible, operationGroups[oIndex][tIndex]);
					}
					*/
					
					if(dirForward){
						for(int j = i - 1; j >= 0; j--){//estabilish a baseline
							Operation t = sorted.get(j);
							if(!o.getResource().equals(t.getResource()))
								continue;
							int tIndex = sortedIndexes[j];
							bestVal = operationGroups[oIndex][tIndex];
							break;
						}
						if(bestVal >= bestPossible)
							continue;
						for(int j = i + 1; j < sorted.size(); j++){
							Operation t = sorted.get(j);
							if(!o.getResourceRange().contains(t.getResource()))
								continue;
							
							offset++;
							int tIndex = sortedIndexes[j];//ops.indexOf(t);
							if(operationGroups[oIndex][tIndex] > bestVal){
								bestOp = t;
								bestVal = operationGroups[oIndex][tIndex];
								if(bestVal >= bestPossible)
									break;
								//if(offset > 1)
								//	break;
							}
						}
					}else{
						for(int j = i + 1; j < sorted.size(); j++){
							Operation t = sorted.get(j);
							if(!o.getResource().equals(t.getResource()))
								continue;
							int tIndex = sortedIndexes[j];
							bestVal = operationGroups[oIndex][tIndex];
							break;
						}
						if(bestVal >= bestPossible)
							continue;
						for(int j = i - 1; j >= 0; j--){
							Operation t = sorted.get(j);
							if(!o.getResourceRange().contains(t.getResource()))
								continue;
							offset++;
							int tIndex = sortedIndexes[j];//ops.indexOf(t);
							if(operationGroups[oIndex][tIndex] > bestVal){
								bestOp = t;
								bestVal = operationGroups[oIndex][tIndex];
								if(bestVal >= bestPossible)
									break;
								//if(offset > 1)
								//	break;
							}
						}
					}
					
					
					
					if( offset > 1 && bestOp != null){
					//if(  bestOp != null){
						Resource alt = bestOp.getResource();
						//if resource is not changed and op is already in place do nothing
//						if(alt.equals(o.getResource())){
//							List<Operation> rops = resourceOperationMap.get(alt);
//							sorted.clear();
//							sorted.addAll(rops);
//							int tIndex = sorted.indexOf(bestOp);
//							assert(tIndex >= 0);
//							Operation tPrev = tIndex > 0 ? sorted.get(tIndex - 1) : null;
//							Operation tNext = tIndex < sorted.size() - 1 ? sorted.get(tIndex + 1) : null;
//							if(tPrev == null && o.getEndDate() <= bestOp.getStartDate())
//								continue;
//							if(tNext == null && o.getStartDate() >= bestOp.getEndDate())
//								continue;
//							if(tPrev != null && o.getStartDate() >= tPrev.getEndDate() && o.getEndDate() <= bestOp.getStartDate())
//								continue;
//							if(tNext != null && o.getStartDate() >= bestOp.getEndDate() && o.getEndDate() <= tNext.getStartDate())
//								continue;
//						}
						
						
						//deliberate overlapp so it there is an op there this one will be inserted
						//long newStart = dirForward ? bestOp.getStartDate() - o.getDuration() - 1 : bestOp.getEndDate();// bestOp.getStartDate() - o.getDuration();
						long newStart = dirForward ?  bestOp.getEndDate() : bestOp.getStartDate() - o.getDuration();
						//if(o.getStartDate() < newStart && acceptMove(o, newStart)){
						if(acceptMove(o, newStart)){
							if(alt.equals(o.getResource()) || acceptMove(o, alt)){
								doMove(calc, o, newStart);
								//sorted.remove(i);
								//sorted.add(o);
								dbgCounter++;								
								mod = true;
								if(!alt.equals(o.getResource())){
									resourceOperationMap.get(o.getResource()).remove(o);
									doMove(calc, o, alt);
									resourceOperationMap.get(alt).add(o);
									dbgCounter++;
									dbgResCounter++;
									mod = true;
								}
								
								
							}
						}else if(1*o.getStartDate() == newStart && !alt.equals(o.getResource()) && acceptMove(o, alt)){
							resourceOperationMap.get(o.getResource()).remove(o);
							doMove(calc, o, alt);
							resourceOperationMap.get(alt).add(o);
							dbgCounter++;
							dbgResCounter++;
							mod = true;
						}
						SortedArrayList<Operation> sortedRops = new SortedArrayList<Operation>(sorted);
						sortedRops.clear();
						sortedRops.addAll(resourceOperationMap.get(alt));
						if(sortedRops.size() > 0){
							long maxEnd = sortedRops.get(sortedRops.size() - 1).getStartDate();
							for(int j = sortedRops.size() - 2; j >= 0; j--){
								Operation t = sortedRops.get(j);
								long overlap = t.getEndDate() - maxEnd;
								if(overlap > 0){
									doMove(calc, t, maxEnd - t.getDuration());
									dbgCounter++;								
									mod = true;
								}
								maxEnd = t.getStartDate();
							}
							
						}
					}
					
					
				}
				moveForward.clear();
				moveForward = null;
				score = calc.calculateScore();
				System.out.println(iteration + " Initialized "+dbgCounter+" / " + dbgResCounter + " items on this consumption matching pass " + score);

				/*
				for(int i = 0; i < sorted.size() ; i++){
					Operation o = sorted.get(i);
					if(!"PIN".equals(o.getResource().getWorkcenter()))
						continue;
					List<StockItemTransaction> trs = o.getWorkOrder().getRequiredTransaction();
					if(trs != null && trs.size() > 0)
						System.out.print(trs);
					System.out.print(" ");
				}
				System.out.println();
				*/
				
				/*
				//find poorly alocated and try to move them forward
				dbgCounter = 0;
				List<String> moveTargets = new ArrayList<String>(ops.size());
				for(Resource r : resourceOperationMap.keySet()){
					List<Operation> rops = resourceOperationMap.get(r);
					sorted.clear();
					sorted.addAll(rops);
					for(int i = 0; i < sorted.size(); i++){
						Operation o = sorted.get(i);
						List<Operation> affinityGroup = operationGroups.get(o); 
						int maxGroup = affinityGroup != null ? affinityGroup.size() : 0;
						if(maxGroup <= 3)
							continue;
						int counter = 0;
						for(int j = i + 1 ; j < sorted.size(); j++){
							if(affinityGroup.contains(sorted.get(j)))
								counter++;
							else
								break;
						}
						for(int j = i - 1 ; j >= 0; j--){
							if(affinityGroup.contains(sorted.get(j)))
								counter++;
							else
								break;
						}
						
						if(counter < 0.3*maxGroup){
							WorkOrder wo = o.getWorkOrder();
							for(StockItemProductionTransaction tr : wo.getProducedTransactionList()){
								String vcr = tr.getVCRNUMORI();
								if(!moveTargets.contains(vcr)){
									moveTargets.add(vcr);
								}
							}
						}
					}
				}
				
				for(String vcr : moveTargets){
					List<WorkOrder> wos = invoiceMap.get(vcr);
					if(wos != null){
						for(WorkOrder wo : wos){
							for(Operation o : wo.getOperations()){
								long targetStart = o.getStartDate() + o.getDuration();
								doMove(calc, o, targetStart);
								mod = true;
								dbgCounter++;
							}
						}
					}
					
				}
				moveTargets.clear();
				score = calc.calculateScore();
				System.out.println(iteration + " Initialized " + dbgCounter + " items on this group optimization pass " + score);
				*/
				
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
					
					long dayT = maxEnd > 0 ? maxEnd/(24*3600) : sorted.get(sorted.size() - 1).getStartDate()/(24*3600);
					dayT *= 24*3600;
					long hour = 17*3600; // set to end at 5 PM
					for(int i = sorted.size() - 1; i >= 0 ; i--){
						Operation o = sorted.get(i);
						long newStart = dayT + hour - o.getDuration();
						/*
						if(o.getWorkOrder().getNextOrder() != null){
							Long fs = o.getWorkOrder().getNextOrder().getFirstStart();
							if(fs != null)
								newStart = Math.min(newStart, 1*fs);
						}
						*/
						//if(Math.abs(newStart - o.getStartDate()) > 0.3*o.getDuration()){ // newStart != o.getStartDate()) ){
						//if(newStart < (o.getStartDate() - 0.3*o.getDuration())){ // newStart != o.getStartDate()) ){
						if(((newStart < 1*o.getStartDate() || o.getStartDate() < newStart - 5*24.0*o.getDuration())) && acceptMove(o, newStart) ){ // newStart != o.getStartDate()) ){
						//if(newStart < 1*o.getStartDate()){
							doMove(calc, o, newStart);
							mod = true;
							dbgCounter ++;
							//hour = newStart - dayT;
						}
						//else
						{
							hour = (o.getStartDate() % (24*3600));// - o.getDuration();
							dayT = o.getStartDate() - hour;
						}
						//hour -= o.getDuration();
						if(hour < 14*3600 && hour > 12.5*3600){
							hour = (long) (12.5*3600);
						}
						if(hour <= 10*3600){
							//the allowed period has expired, so scan an equivalent resource
							//and place it at top time there...
							//if no matching resource is found let it flow
							/*
							if(o.getResourceRange().size() > 1){
								Resource alt = null;
								for(int ri = resIdx + 1 ; ri < res.size(); ri++){
									if(o.getResourceRange().contains(res.get(ri))){
										alt = res.get(ri);
										break;
									}
								}
								if(alt != null){
									doMove(calc, o, alt);
									doMove(calc, o, dayT + 17*3600 - o.getDuration());
									resourceOperationMap.get(o.getResource()).remove(o);
									resourceOperationMap.get(alt).add(o);
									sorted.remove(o);
									rops.remove(o);
									mod = true;
									dbgCounter ++;
								}
								
							}
							*/
							
							hour = 17*3600;
					        cal.setTimeInMillis(dayT*1000);
					        int idx = cal.get(Calendar.DAY_OF_WEEK);	//note that Calendar.SUNDAY = 1
					        if(idx == 2)
					        	dayT -= 3*24*3600;
					        else if(idx == 1)
					        	dayT -= 2*24*3600;
					        else
					        	dayT -= 24*3600;
						}
					}
				}
				score = calc.calculateScore();
				System.out.println(iteration + " Initialized "+dbgCounter+" items on this capacity normalization pass " + score);
				/*
				dbgCounter = 0;
				//TODO: useless step
				for(Operation o : ops){
					WorkOrder wo = o.getWorkOrder();
					Long end = wo.getExecutionGroupForcedEnd();
					
					if(end != null && o.getEndDate() > end){
						Long newStart = end - o.getDuration();
						doMove(calc, o, newStart);
						mod = true;
						dbgCounter++;
					}
				}
				System.out.println(iteration + " Initialized "+dbgCounter+" items on this execution group end pass");
				*/
				dbgCounter = 0;
				for(WorkOrder wo : allWo){
					
					Long firstStart = wo.getExecutionGroupForcedEnd();
					WorkOrder n = wo.getNextOrder();
					if(n != null){
						firstStart = firstStart == null ? n.getFirstStart() : Math.min(n.getFirstStart(), firstStart);
					}
					if(firstStart == null)
						continue;
					for(Operation o : wo.getOperations()){
						if(o.getEndDate() > firstStart){
							Long newStart = firstStart - o.getDuration();
							if(o.isMovable()){
								doMove(calc, o, newStart);
								dbgCounter++;								
								mod = true;
								
								sorted.clear();
								sorted.addAll(resourceOperationMap.get(o.getResource()));
								int oIndex = sorted.indexOf(o);
								long maxEnd = o.getStartDate();
								for(int j = oIndex - 1; j >= 0; j--){
									Operation t = sorted.get(j);
									long overlap = t.getEndDate() - maxEnd;
									if(overlap > 0){
										doMove(calc, t, maxEnd - t.getDuration());
										dbgCounter++;								
										mod = true;
									}
									maxEnd = t.getStartDate();
								}
								
							}
							 
						}
					}
					
				}
				
//				for(String vcr : invoiceMap.keySet()){
//					List<WorkOrder> woList = invoiceMap.get(vcr);
//					for(WorkOrder wo : woList){
//						
//						if(wo.getProducedTransactionList().size() == 0)
//							continue;
//						
//						List<StockItem> producedItems = new ArrayList<StockItem>(wo.getProducedTransactionList().size());
//						for(StockItemTransaction tr : wo.getProducedTransactionList()){
//							producedItems.add(tr.getItem());
//						}
//						List<WorkOrder> consumers = new ArrayList<WorkOrder>();
//						for(WorkOrder p : woList){
//							List<StockItemTransaction> reqs = p.getRequiredTransaction();
//							for(StockItemTransaction r : reqs){
//								if(producedItems.contains(r.getItem()) && ! consumers.contains(p))
//									consumers.add(p);
//							}
//						}
//						
//						long firstConsumer = Long.MAX_VALUE;
//						for(WorkOrder c : consumers){
//							if(c.getFirstStart() < firstConsumer)
//								firstConsumer = c.getFirstStart();
//							/*
//							if(c.getOperations().isEmpty())
//								continue;
//							for(Operation o : c.getOperations()){
//								if(o.getStartDate() < firstConsumer){
//									firstConsumer = o.getStartDate();
//									wo.setNextOrder(c);
//								}
//							}
//							*/
//						}
//						//second step, place all operations behind any forced end group
//						Long minEnd =  wo.getExecutionGroupForcedEnd();
//						if(minEnd != null && minEnd < firstConsumer)
//							firstConsumer = minEnd;
//
//						if(firstConsumer >= Long.MAX_VALUE){
//							firstConsumer = System.currentTimeMillis()/1000;
//						}
//						
//						if(firstConsumer < Long.MAX_VALUE){
//							for(Operation o : wo.getOperations()){
//								Long newStart = firstConsumer - o.getDuration();
//								if(o.isMovable() && o.getEndDate() > firstConsumer ){
//									doMove(calc, o, newStart);
//									dbgCounter++;								
//									mod = true;
//								}
//								
//							}
//						}
//						
//						
//					}
//					
//					
//				}
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
						if(mBestScore == null || score.compareTo(mBestScore) > 0){
							mBestScore = score;
							setBestSchedule(mBestScore, mSchedule);
						}
					}finally {
						mBestScoreSync.unlock();
					}
				}
				
				if(!mod){
					//no more optimization possible
					for(Operation o : lastMoves.keySet()){
						if(lastMoves.containsKey(o))
							lastMoves.get(o).clear();
						if(lastResourceMoves.containsKey(o))
							lastResourceMoves.get(o).clear();
					}
					calc.resetWorkingSolution(mSchedule);
				}
				
				if(!mod){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
					
				
				iteration++;
			}while(true);
			
			
			
			
		}
		
		
		
		private void updateNextWorkOrders(Map<String, List<WorkOrder>> invoiceMap) {
			
			for(String vcr : invoiceMap.keySet()){
				List<WorkOrder> woList = invoiceMap.get(vcr);
				for(WorkOrder wo : woList){
					if(wo.getProducedTransactionList().size() == 0)
						continue;
					
					List<StockItem> producedItems = new ArrayList<StockItem>(wo.getProducedTransactionList().size());
					for(StockItemTransaction tr : wo.getProducedTransactionList()){
						producedItems.add(tr.getItem());
					}
					List<WorkOrder> consumers = new ArrayList<WorkOrder>();
					for(WorkOrder p : woList){
						List<StockItemTransaction> reqs = p.getRequiredTransaction();
						for(StockItemTransaction r : reqs){
							if(producedItems.contains(r.getItem()) && ! consumers.contains(p)){
								consumers.add(p);
							}
						}
					}
					
					long firstConsumer = Long.MAX_VALUE;
					for(WorkOrder c : consumers){
						for(Operation o : c.getOperations()){
							if(o.getStartDate() < firstConsumer){
								firstConsumer = o.getStartDate();
								wo.setNextOrder(c);
							}
						}
					}
					
					if(wo.getNextOrder() == null){
						System.out.println("No next wo for "+wo.toString()+" : "+wo.getProducedTransactionList());
					}
					
				}
			}
			
		}

		public void run_() {
			mRunning = true;
			List<Operation> ops = mSchedule.getOperationList();
			List<Resource> resources = mSchedule.getResourceList();
			
			OperationIncrementalScoreCalculator calc = new  OperationIncrementalScoreCalculator();
			
			calc.resetWorkingSolution(mSchedule);
			
			Random r = new Random(System.currentTimeMillis());
			
			long now = System.currentTimeMillis()/1000;
			long min = now;
			long max = now + 30*24*3600;
			//Score lastScore = null;
			Score lastBestLocalScore = null;
			Score lastLocalScore = null;
			Move capturedMove = null;
			
			List<Momentum> sortedIndexes = new ArrayList<Momentum>(ops.size());
			for(int i = 0; i < ops.size(); i++){
				Momentum m = new Momentum();
				m.OperationIndex = i;
				m.Gradient = 0;//r.nextGaussian();
				sortedIndexes.add(m);
			}
			
			for(Operation o : ops){
				if(o.getStartDate() < min)
					min = o.getStartDate();
			}
			
			min -= 15*24*3600;
			int nonImprovingCount = 0;
			
			SortedArrayList<Score> scoreSorter = new SortedArrayList<Score>(new Comparator<Score>() {
				@Override
				public int compare(Score o1, Score o2) {
					return -1*o1.compareTo(o2);	//best first
				}
			});

			lastBestLocalScore = calc.calculateScore();
			log("Starting score :" + lastBestLocalScore);
			lastLocalScore = lastBestLocalScore;
			int scoreIndex = -1;
			synchronized (mTopScores) {
				mTopScores.add(lastLocalScore);
				scoreIndex = mTopScores.size() - 1;
			}
			
			Map<String, Set<Operation>> vcrOperations  = new HashMap<String, Set<Operation>>();
			for(Operation o : ops){
				WorkOrder wo = o.getWorkOrder();
				for(StockItemProductionTransaction tr : wo.getProducedTransactionList()){
					String vcr = tr.getVCRNUMORI();
					Set<Operation> set = vcrOperations.get(vcr);
					if(set == null){
						set = new HashSet<>();
						vcrOperations.put(vcr, set);
					}
					set.add(o);
				}
			}
			List<Move> moves = new ArrayList<Move>(10);
			
			ScoreDirector dummy = new NullScoreDirector(mSchedule);
			ScheduleSolutionInitializer init = new ScheduleSolutionInitializer();
			

			
			int opIdx = -1;
			while(mRunning){
				r.setSeed(System.currentTimeMillis());
				boolean rollback = true;
				moves.clear();
				
				//double ri = 1 - Math.abs(r.nextGaussian());
				//opIdx = (int)(ri*sortedIndexes.size()+0.5-1);
				//Momentum momentum = sortedIndexes.get((int)Math.max(opIdx, 0));
				opIdx++;
				if(opIdx >= sortedIndexes.size())
					opIdx = 0;
				Momentum momentum = sortedIndexes.get(opIdx);
				opIdx = momentum.OperationIndex;
				//int opIdx = r.nextInt(ops.size());
				//int resIdx = -1;
				
				Operation o = ops.get(opIdx);
				//WorkOrder wo = o.getWorkOrder();
				//Resource resource = null;
				
				Move move = null;
				
				capturedMove = mGoodMoves.poll();
				if(capturedMove != null && capturedMove.ThreadId != Thread.currentThread().getId() ){// capturedMove.Rank >= mRanking){
					mGoodMoves.offer(capturedMove);
					capturedMove = null;
				}

				if(capturedMove == null){
					move =  new Move(o, opIdx, mRanking);
					
					move.ResourceIndex = r.nextInt(move.Operation.getResourceRange().size());
					move.Resource = move.Operation.getResourceRange().get(move.ResourceIndex);

					long rstart = 0;
					//gessing next move
					//5% of the time we scan overlapps and enforce move
					double p = r.nextDouble();
					if(p < 0.00){
						for(Operation op : ops){
							if(op.equals(o))
								continue;
							if(move.Resource .equals(op.getResource())){
								if(o.getStartDate() < op.getEndDate() && op.getStartDate() < o.getEndDate()){
									rstart = op.getStartDate() - o.getDuration();
									rollback = false;
									log("Enforcing overlapp solving");
									break;
								}
							}
						}
					}
					if(rstart <= 0 && p < 0.4){
						Operation s = ops.get(r.nextInt(ops.size()));
						//if(r.nextBoolean())
						if(momentum.Gradient > 0)
							rstart = s.getEndDate();
						else
							rstart = s.getStartDate() - o.getDuration();
					}
					if(rstart <= 0 && p < 0.8 && Math.abs(momentum.Gradient) > 1){
						rstart = (long) (o.getStartDate() + momentum.Gradient);//*Math.abs(r.nextGaussian() + 1));
					}
					if(rstart < min || rstart > max)
						rstart = (long)(min + r.nextDouble()*(max-min));
					
					move.setStartTime( rstart );
					
				}else{
					//log("grabbed a move from someone else");
					rollback = false;
					opIdx = capturedMove.OperationIndex; 
					o = ops.get(opIdx);
					move =  new Move(o, opIdx, mRanking);
					move.setStartTime(capturedMove.StartTime);
					move.ResourceIndex = capturedMove.ResourceIndex;
					move.Resource = move.Operation.getResourceRange().get(move.ResourceIndex);
					for(Momentum m : sortedIndexes){
						if(m.OperationIndex == move.OperationIndex){
							momentum = m;
							break;
						}
					}
				}
				moves.add(move);
				/*
				long rstart = 0;
				
				if(rstart <= 0){
					
					List<Resource> res = o.getResourceRange();
					resIdx = r.nextInt(res.size());
					resource = res.get(resIdx);
					
					
					
					
					
					
				}
				*/
				/*
				//add moves for adjacent ops
				if(move.StartTime != move.Operation.getStartDate()){
					long offset = move.StartTime - move.Operation.getStartDate();
					String vcr = move.Operation.getWorkOrder().getProducedTransactionList().get(0).getVCRNUMORI();
					Set<Operation> adj = vcrOperations.get(vcr);
					if(adj != null && adj.size() > 1){
						SortedArrayList<Operation> sorted = new SortedArrayList<Operation>(new Comparator<Operation>() {
							@Override
							public int compare(Operation o1, Operation o2) {
								return o1.getStartDate().compareTo(o2.getStartDate());
							}
						});
						sorted.addAll(adj);
						int startIdx = sorted.indexOf(move.Operation);
						assert(startIdx >= 0);
						if(move.StartTime > move.Operation.getStartDate()){
							//moving forward
							long latestStart = move.StartTime + move.Operation.getDuration();
							for(int i = startIdx + 1 ; i < sorted.size(); i++){
								Operation tgt = sorted.get(i);
								if(tgt.getStartDate() < latestStart){
									Move m = new Move(tgt);
									m.setStartTime(latestStart);
									moves.add(m);
									latestStart += tgt.getDuration();
								}else{
									break;
								}
							}
						}else{
							//moving backwards
							long latestEnd = move.StartTime;
							for(int i = startIdx - 1 ; i >= 0; i--){
								Operation tgt = sorted.get(i);
								if(tgt.getEndDate() > latestEnd){
									Move m = new Move(tgt);
									m.setStartTime(latestEnd - tgt.getDuration());
									moves.add(m);
									latestEnd -= tgt.getDuration();
								}else{
									break;
								}
							}
						}
						
					}
					
				}
				*/
				
				boolean moved = false;
				
				for(Move m : moves){
					
					if(m.StartTime != m.Operation.getStartDate()){
						moved = true;
						WorkOrder mWo = m.Operation.getWorkOrder();
						calc.beforeVariableChanged(m.Operation, "startDate");
						calc.beforeVariableChanged(mWo, "firstStart");
						
						
						
						m.Operation.setStartDate(m.StartTime);
						
						mWo.setFirstStart(m.FirstStart);
						mWo.setLastEnd(m.LastEnd);
						
						calc.afterVariableChanged(m.Operation, "startDate");
						calc.afterVariableChanged(mWo, "firstStart");
					}
					Resource res = m.Resource;
					if(!res.equals(m.Operation.getResource())){
						moved = true;
						calc.beforeVariableChanged(m.Operation, "resource");
						m.Operation.setResource(res);
						calc.afterVariableChanged(m.Operation, "resource");
					}
				}
					
				if(!moved)
					continue;
				
				Score score = calc.calculateScore();
				
				
//				System.out.println("- " + score);
				if(mBestScoreSync.tryLock()){
					try{
						if(mBestScore == null || score.compareTo(mBestScore) > 0){
							mBestScore = score;
							setBestSchedule(score, mSchedule);
						}
					}finally {
						mBestScoreSync.unlock();
					}
				}
				
				boolean improve = false;
				if(lastLocalScore == null)
					improve = true;
				else if(score.compareTo(lastBestLocalScore) > 0)
					improve = true;
				
				
				if(improve){
					rollback = false;
					lastBestLocalScore = score;
					nonImprovingCount = 0;
				}else{
					nonImprovingCount++;
				}
				
				if(!rollback){
					
					scoreSorter.clear();
					synchronized (mTopScores) {
						mTopScores.set(scoreIndex, score);
						scoreSorter.addAll(mTopScores);
						mRanking = scoreSorter.indexOf(score);
					}
					
					if(capturedMove == null){
						mGoodMoves.offer(move);
					}
					
				}
				
				//if(capturedMove == null){					
					if(lastLocalScore != null){
						double e = sum(score) - sum(lastLocalScore); // distance(score, lastLocalScore);
						double d = move.StartTime - move.Rollback.StartTime;
						double g = momentum.Gradient;
						momentum.Gradient += 0.001*e*d;
						if(Math.abs(momentum.Gradient) > 5*24*3600)
							momentum.Gradient = g;
						/*
						int comp = score.compareTo(lastLocalScore); 
						if(comp > 0){
							//improvement
							momentum.Gradient += 0.01*d*e;
							//momentum.Gradient += 0.1*d*(momentum.Gradient == 0 ? 0.1 : momentum.Gradient);
						}else if(comp < 0){
							momentum.Gradient -= 0.01*d*e;
							//momentum.Gradient -= 0.1*d*(momentum.Gradient == 0 ? 0.1 : momentum.Gradient);
						}
						*/
					}
				//}
				
				
				
				
				if(!rollback){
					//this was an improving move so put on the end so we'll select it again
					//sortedIndexes.remove(momentum);
					//sortedIndexes.add(momentum);
				}
				
				
				//if it's a low ranking thread, accept bad moves once in a while
				//double acceptBadProbability = mRanking > 0 ? 0.5*(mRanking/(double)TOP_LIST_SIZE) : -1.0;
				double acceptBadProbability = (double)(mRanking*nonImprovingCount/1000.0);
				
				
				
				if(rollback && (r.nextDouble() > acceptBadProbability)){
					nonImprovingCount = 0;
					for(Move m : moves){
						Move rollbackMove = m.Rollback;
						Operation ro = rollbackMove.Operation;
						WorkOrder rWo = ro.getWorkOrder();
						//assert(ranking > 0);
						if(rollbackMove.StartTime != ro.getStartDate()){
							calc.beforeVariableChanged(ro, "startDate");
							calc.beforeVariableChanged(rWo, "firstStart");
							ro.setStartDate(rollbackMove.StartTime);
							rWo.setFirstStart(rollbackMove.FirstStart);
							rWo.setLastEnd(rollbackMove.LastEnd);
							calc.afterVariableChanged(ro, "startDate");
							calc.afterVariableChanged(rWo, "firstStart");
						}
						Resource rres = rollbackMove.Resource;
						if(!rres.equals(ro.getResource())){
							calc.beforeVariableChanged(ro, "resource");
							ro.setResource(rres);
							calc.afterVariableChanged(ro, "resource");
						}
					
					}
					
					score = calc.calculateScore();
					//assert(score.equals(lastLocalScore));
					
				}else if(rollback){
					log("BAD score accepted any how");
				}
				
				lastLocalScore = score;
				
				//if(mRanking > 3)
				{
					
					try {
						
						long t = (long)((mRanking/(double)THREAD_COUNT)*3000.0);
						Thread.sleep(t);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
			}
			
		}
		
		private double sum(Score score){
			Number[] s1Levels = score.toLevelNumbers();
			
			int n = s1Levels.length;
			double res = 0;
			for(int i = 0; i < n; i++){
				double s1V = s1Levels[i].doubleValue();
				res += s1V;
			}
			return res;
			
		}
		
		private int compareLevels(Score s1, Score s2, int level){
			Number[] s1Levels = s1.toLevelNumbers();
			Number[] s2Levels = s2.toLevelNumbers();
			
			int n = s1Levels.length;
			int res = -1;
			int start = 0;
			int end = n;
			if(level >= 0){
				start = level;
				end = start + 1;
			}
			for(int i = start; i < end; i++){
				double s1V = s1Levels[i].doubleValue();
				double s2V = s2Levels[i].doubleValue();
				if(s1V > s2V)
					return 1;
				if(s1V == s2V)
					res = 0;
			}
			return res;
			
		}
		
		private double distance(Score s1, Score s2){
			Number[] s1Levels = s1.toLevelNumbers();
			Number[] s2Levels = s2.toLevelNumbers();
			double sum = 0;
			int n = s1Levels.length;
			for(int i = 0; i < n; i++){
				double s1V = s1Levels[i].doubleValue();
				double s2V = s2Levels[i].doubleValue();
				double den = Math.max(s1V, s2V);
				if(den != 0){
					double d = (s1V - s2V)/den;
					sum += d*d;
				}
			}
			return Math.sqrt(sum);
		}
		
		private int[] compare(Score s1, Score s2){
			
			
			Number[] s1Levels = s1.toLevelNumbers();
			Number[] s2Levels = s2.toLevelNumbers();
			int[] res = new int[s1Levels.length];
					
			for(int i = 0; i < s1Levels.length; i++){
				double s1V = s1Levels[i].doubleValue();
				double s2V = s2Levels[i].doubleValue();
				
				if(s1V > s2V){
					res[i] = 1;
				}else if(s2V < s1V){
					res[i] = -1;
				}
				
			}
			return res;
			
		}
		
		
		
		
	}
	

}
