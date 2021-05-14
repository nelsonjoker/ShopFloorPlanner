package pt.sotubo.planner.solver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.optaplanner.core.impl.domain.variable.descriptor.GenuineVariableDescriptor;
import org.optaplanner.core.impl.heuristic.move.AbstractMove;
import org.optaplanner.core.impl.heuristic.move.Move;
import org.optaplanner.core.impl.heuristic.selector.move.factory.MoveIteratorFactory;
import org.optaplanner.core.impl.heuristic.selector.move.generic.ChangeMove;
import org.optaplanner.core.impl.score.director.ScoreDirector;

import pt.sotubo.planner.SortedArrayList;
import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Resource;
import pt.sotubo.planner.domain.Schedule;
import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemProductionTransaction;
import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;

public class CustomChangeMoveFactory implements MoveIteratorFactory {

	private class MoveIterator implements Iterator<Move>{
		
		public class CustomChangeMove extends AbstractMove{
			protected final Object entity;
			protected final Long toPlanningValue;
		   
			public CustomChangeMove(Operation entity, Long toPlanningValue){
				this.entity = entity;
				this.toPlanningValue = toPlanningValue;
						//super(entity, null, toPlanningValue);
			}
			public Operation getOperation() {
				return (Operation)entity;
			}
			public Long getTargetValue() {
				return (Long)toPlanningValue;
			}
			public boolean isMoveDoable(ScoreDirector scoreDirector){
				Object oldValue = getOperation().getStartDate();
				return !ObjectUtils.equals(oldValue, getTargetValue());
			}
			
			public Move createUndoMove(ScoreDirector scoreDirector) {
				Long oldValue = getOperation().getStartDate();
				return new CustomChangeMove(getOperation(), oldValue);
			}
			protected void doMoveOnGenuineVariables(ScoreDirector scoreDirector){
				scoreDirector.beforeVariableChanged(entity, "startDate");
				getOperation().setStartDate(getTargetValue());
				scoreDirector.afterVariableChanged(entity, "startDate");
			}
			public String getSimpleMoveTypeDescription(){
				return getClass().getSimpleName() + "(startDate)";
			}
			public boolean equals(Object o) {
				if (this == o)
					return true;
				if ((o instanceof CustomChangeMove)) {
					CustomChangeMove other = (CustomChangeMove)o;
					return new EqualsBuilder().append(entity, other.entity).append(getTargetValue(), other.getTargetValue()).isEquals();
				}
				return false;
			}
			public int hashCode(){
				return new HashCodeBuilder().append(entity).append(getTargetValue()).toHashCode();
			}
			public String toString(){
				Long oldValue = getOperation().getStartDate();
				return entity + " {" + oldValue + " -> " + getTargetValue() + "}";
			}
			@Override
			public Collection<? extends Object> getPlanningEntities() {
				return Collections.singletonList(entity);
			}
			@Override
			public Collection<? extends Object> getPlanningValues() {
				return Collections.singletonList(getTargetValue());
			}
					
		}
		
		public class MoveBefore extends CustomChangeMove{
			private Operation mTarget;
			public MoveBefore(Operation entity, Operation target){
				super(entity, target.getStartDate() - entity.getDuration());
				mTarget = target;
			}
			
			@Override
			public Long getTargetValue() {
				return mTarget.getStartDate() - getOperation().getDuration();
			}
		}
		public class MoveAfter extends CustomChangeMove{
			private Operation mTarget;
			public MoveAfter(Operation entity, Operation target){
				super(entity, target.getEndDate());
				mTarget = target;
			}
			
			@Override
			public Long getTargetValue() {
				return mTarget.getEndDate();
			}
		}
		
		public class BlockChangeMove extends AbstractMove{

			private List<CustomChangeMove> mMoves;
			
			public BlockChangeMove(){
				mMoves = new ArrayList<>(16);
			}
			
			public void add(Operation entity, Long toPlanningValue){
				CustomChangeMove mv = new CustomChangeMove(entity, toPlanningValue);
				mMoves.add(mv);
			}
			
			
			@Override
			public boolean isMoveDoable(ScoreDirector paramScoreDirector) {
				return !mMoves.stream().anyMatch(mv -> mv.isMoveDoable(paramScoreDirector) == false);
			}

			@Override
			public Move createUndoMove(ScoreDirector paramScoreDirector) {
				BlockChangeMove undo = new BlockChangeMove();
				for(CustomChangeMove m : mMoves)
					undo.add(m.getOperation(), m.getOperation().getStartDate());
				return undo;
			}

			@Override
			public Collection<? extends Object> getPlanningEntities() {
				return mMoves;
			}

			@Override
			public Collection<? extends Object> getPlanningValues() {
				List<Long> values = new ArrayList<Long>(mMoves.size());
				for(CustomChangeMove mv : mMoves){
					values.add(mv.getTargetValue());
				}
				return values;
			}

			@Override
			protected void doMoveOnGenuineVariables(ScoreDirector paramScoreDirector) {
				for(CustomChangeMove mv : mMoves){
					mv.doMoveOnGenuineVariables(paramScoreDirector);
				}
				
			}
		}
		
		

		private Schedule mSchedule;
		private Random mRandom;
		private List<Move> mNextMoves;

		public MoveIterator(Schedule s, Random r) {
			mSchedule = s;
			mRandom = r;
			mNextMoves = new ArrayList<Move>();
		}

		@Override
		public boolean hasNext() {
			if(mNextMoves.isEmpty())
				prepareNextMoves();
			
			return mNextMoves.size() > 0;
		}

		

		@Override
		public Move next() {
			if(mNextMoves.isEmpty())
				prepareNextMoves();
			int idx = mRandom.nextInt(mNextMoves.size());
			Move m = mNextMoves.get(idx);
			mNextMoves.remove(idx);
			return m;
		}
		
		private int prepareNextMoves() {
			
			List<WorkOrder> allWo = mSchedule.getWorkOrderList();
			
			Map<String, List<Operation>> invoiceMap = new HashMap<String, List<Operation>>();
			for(WorkOrder wo  : allWo){
				for (StockItemProductionTransaction stockProduction : wo.getProducedTransactionList()) {
					List<Operation> oList = invoiceMap.get(stockProduction.getVCRNUMORI());
					if(oList == null){
						oList = new LinkedList<>();
						invoiceMap.put(stockProduction.getVCRNUMORI(), oList);
					}
					oList.addAll(wo.getOperations());
				}
				Long end = wo.getExecutionGroupForcedEnd();
				if(end != null){
					//List<Operation> delayed = wo.getOperations().stream().filter(o -> (o.getEndDate() > end)).collect(Collectors.toList());
					for(Operation op : wo.getOperations()){
						//Operation op = (Operation)o;
						if(op.getEndDate() > end)
							mNextMoves.add(new CustomChangeMove(op, end - op.getDuration()));
					}
					/*
					Iterator<Operation> it = delayed.iterator();
					while(it.hasNext()){
						Operation o = it.next();
						mNextMoves.add(new CustomChangeMove(o, end - o.getStartDate()));
					}
					*/
				}
			}
			
			for(String vcr : invoiceMap.keySet()){
				List<Operation> oList = invoiceMap.get(vcr);
				BlockChangeMove blockDelay = new BlockChangeMove();
				BlockChangeMove blockShrink = new BlockChangeMove();
				BlockChangeMove blockExpand = new BlockChangeMove();
				int hp = oList.size() / 2;
				int i = 0;
				for(Operation op : oList){
					blockDelay.add(op, op.getStartDate() - op.getDuration());
					if(i >= hp){
						blockShrink.add(op, op.getStartDate()-op.getDuration());
						blockExpand.add(op, op.getStartDate()+op.getDuration());
					}else{
						blockShrink.add(op, op.getStartDate()+op.getDuration());
						blockExpand.add(op, op.getStartDate()-op.getDuration());
					}
					i++;
				}
				mNextMoves.add(blockDelay);
				mNextMoves.add(blockShrink);
				mNextMoves.add(blockExpand);
			}
			
			//prepareNextMoves_();
			prepareOverlappedMoves();
			
			return mNextMoves.size();
		}
		
		private int prepareOverlappedMoves(){
			//spot overlappings and propose shifts...
			int retval = mNextMoves.size();
			Map<Resource, List<Operation>> resourceOperations = new HashMap<>();
			List<Resource> resources = mSchedule.getResourceList();
			List<Operation> operations = mSchedule.getOperationList();
					
			for(Resource res : resources){
				List<Operation> list = new SortedArrayList<Operation>(new Comparator<Operation>() {

					@Override
					public int compare(Operation o1, Operation o2) {
						return (int)((o1.getStartDate() - o2.getStartDate()));
					}
				});
				resourceOperations.put(res, list);
				for(Operation op : operations){
					if(op.getResource().equals(res)){
						list.add(op);
					}
				}
			}
			
			
			for(Resource res : resources){
				List<Operation> ops = resourceOperations.get(res);
				
				TreeMap<Long, List<Operation>> opsPerDay = new TreeMap<Long, List<Operation>>();
				for(Operation o : ops){
					long date = o.getStartDate()/(24*3600);
					List<Operation> list = opsPerDay.get(date);
					if(list == null){
						list = new ArrayList<>();
						opsPerDay.put(date, list);
					}
					list.add(o);
				}
				
				//max capacity day?
				long maxUsage = -1;
				Long maxUsageDay = null;
				for(Long k : opsPerDay.keySet()){
					List<Operation> list = opsPerDay.get(k);
					long usage = 0;
					for(Operation o : list){
						usage += o.getResourceRequirement();
					}
					if(usage > maxUsage){
						maxUsage = usage;
						maxUsageDay = k;
					}
				}
				if(maxUsage > 0){
					List<Operation> list = opsPerDay.get(maxUsageDay);
					Entry<Long, List<Operation>> nextEntry = opsPerDay.higherEntry(maxUsageDay);
					Entry<Long, List<Operation>> previousEntry = opsPerDay.lowerEntry(maxUsageDay);
					
					List<Operation> next = nextEntry == null ? null : nextEntry.getValue();
					List<Operation> previous = previousEntry == null ? null : previousEntry.getValue();
					
					for(Operation o : list){
						if(next != null){
							Operation tgt = next.get(mRandom.nextInt(next.size()));
							mNextMoves.add(new MoveAfter(o, tgt));
						}
						if(previous != null){
							Operation tgt = previous.get(mRandom.nextInt(previous.size()));
							mNextMoves.add(new MoveBefore(o, tgt));
						}
					}
				}
				
				
				for(int i = ops.size() - 1; i >= 1; i--){
					Operation op = ops.get(i);
					Operation prev = ops.get(i-1);
					long overlap = prev.getEndDate() - op.getStartDate();
					if(overlap != 0){
						mNextMoves.add(new MoveBefore(prev, op));
					}
				}
			}
			
			return mNextMoves.size() - retval;
		}
		
		
		private int prepareNextMoves_() {
			
			//put all operations in execution group boundaries
			List<Operation> ops = mSchedule.getOperationList();
			for(Operation o : ops){
				
				if(!o.isMovable())
					continue;
				
				Long start = o.getStartDate();
				Long end = o.getEndDate();
				WorkOrder wo = o.getWorkOrder();
				Long fs = wo.getExecutionGroupForcedStart();
				Long fe = wo.getExecutionGroupForcedEnd();
				if(fs != null && start < fs){
					mNextMoves.add( new CustomChangeMove(o, new Long(fs) ));
					//return mNextMoves.size();
				}
				if(fe != null && end > fe){
					mNextMoves.add( new CustomChangeMove(o, new Long(fe - o.getDuration())) );
					//return mNextMoves.size();
				}
				
			}
			
			//step 2 ensure supplies before demands
			Map<String, List<WorkOrder>> invoiceMap = new HashMap<String, List<WorkOrder>>();
			List<WorkOrder> allWo = mSchedule.getWorkOrderList();
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
			
			for(String vcr : invoiceMap.keySet()){
				List<WorkOrder> woList = invoiceMap.get(vcr);
				BlockChangeMove blockDelay = new BlockChangeMove();
				for(WorkOrder wo : woList){
					if(wo.getProducedTransactionList().size() == 0)
						continue;
					
					for(Operation op : wo.getOperations()){
						blockDelay.add(op, op.getStartDate() - op.getDuration());
					}
					
					StockItemProductionTransaction stockProduction = wo.getProducedTransactionList().get(0);
					List<WorkOrder> consumers = new LinkedList<WorkOrder>();
					for(WorkOrder p : woList){
						List<StockItemTransaction> reqs = p.getRequiredTransaction();
						for(StockItemTransaction r : reqs){
							if(r.getItem().equals(stockProduction.getItem()) && ! consumers.contains(p))
								consumers.add(p);
						}
					}
					long firstConsumer = Long.MAX_VALUE;
					
					if(consumers.size() > 0){
						for(WorkOrder c : consumers){
							for(Operation o : c.getOperations()){
								firstConsumer = o.getStartDate();
								for(Operation po : wo.getOperations()){
									if(po.isMovable() && po.getEndDate() > firstConsumer){
										mNextMoves.add( new MoveBefore(po, o) );
										//mNextMoves.add( new CustomChangeMove(po, new Long(firstConsumer - po.getDuration())) );
										//return mNextMoves.size();
									}
								}
							}
						}
					}
					
					//if(mNextMoves.size() > 0)
					//	return mNextMoves.size();
					
					/*
					for(WorkOrder c : consumers){
						if(c.getOperations().isEmpty())
							continue;
						for(Operation o : c.getOperations()){
							if(o.getStartDate() < firstConsumer){
								firstConsumer = o.getStartDate();
							}
						}
					}
					
					if(firstConsumer < Long.MAX_VALUE){
						for(Operation o : wo.getOperations()){
							if(o.isMovable() && o.getEndDate() > firstConsumer){
								mNextMoves.add( new CustomChangeMove(o, new Long(firstConsumer - o.getDuration())) );
								return 1;
							}
						}
					}
					*/
				}
				mNextMoves.add(blockDelay);
			}
			
			//return mNextMoves.size();
			
			//step 3, group produced items  
			List<Operation> sortedOps = new ArrayList<Operation>(ops);
			sortedOps.sort(new Comparator<Operation>() {
				@Override
				public int compare(Operation o1, Operation o2) {
					return (int) (o2.getEndDate() - o1.getEndDate());	//reverse order (recent first to old last)
				}
			});
			
			List<Resource> res = mSchedule.getResourceList();
			Map<Resource, List<Operation>> resourceOperationMap = new HashMap<Resource, List<Operation>>();
			for(Resource r : res){
				resourceOperationMap.put(r, new ArrayList<Operation>());
			}
			for(Operation o : sortedOps){
				resourceOperationMap.get(o.getResource()).add(o);
			}
			List<StockItem> items = new ArrayList<StockItem>();
			List<StockItem> test = new ArrayList<StockItem>();
			List<StockItem> itemsProduced = new ArrayList<StockItem>();
			List<StockItem> testProduced = new ArrayList<StockItem>();
			for(Resource r : res){
				List<Operation> rops = resourceOperationMap.get(r);
				if(rops.size() < 2)
					continue;
				//check if prev has same produced as next
				//otherwise find a better next if possible
				Operation next = null;
				Operation prev = null;
				for(int i = 0; i < rops.size()-1; i++){
					next = rops.get(i);
					long maxStartTime = -1;
					long minStartTime = -1;
					
					StockItem produced = next.getWorkOrder().getProducedTransactionList().size() > 0 ? next.getWorkOrder().getProducedTransactionList().get(0).getItem() : null;
					assert(produced != null);
					if(produced == null)
						continue;
					String VCRNUM = next.getWorkOrder().getProducedTransactionList().get(0).getVCRNUMORI();
					
					List<StockItem> consumed = new ArrayList<StockItem>(next.getWorkOrder().getRequiredTransaction().size());
					for(StockItemTransaction tr : next.getWorkOrder().getRequiredTransaction()){
						if(!consumed.contains(tr.getItem()))
							consumed.add(tr.getItem());
					}
					
					if(consumed.size() == 0)
						continue;
					
					for(Operation o : sortedOps){
						String oVCRNUM = o.getWorkOrder().getProducedTransactionList().get(0).getVCRNUMORI();
						if(!VCRNUM.equals(oVCRNUM))
							continue;
						if(o.getStartDate() > next.getEndDate()){
							for(StockItemTransaction tr : o.getWorkOrder().getRequiredTransaction()){
								if(produced.equals(tr.getItem()))
									maxStartTime = o.getStartDate()-next.getDuration();
							}
						}
						if(o.getEndDate() < next.getStartDate()){
							for(StockItemTransaction tr : o.getWorkOrder().getProducedTransactionList()){
								if(consumed.contains(tr.getItem())){
									minStartTime = o.getEndDate();//sorted list so we can break at first one
									break;
								}
							}
						}
						if(minStartTime > 0)
							break;	//sorted list so we can break at first one
					}
					
					if(maxStartTime < 0 || minStartTime < 0)
						continue;
					
					assert(minStartTime <= maxStartTime);
					//TODO: redundant
					items.clear();
					for(StockItemTransaction tr : next.getWorkOrder().getRequiredTransaction()){
						if(!items.contains(tr.getItem()))
							items.add(tr.getItem());
					}
					
					int lowestValue = Integer.MAX_VALUE;
					int lowestIndex = -1;
					int prevCount = 0;
					prev = rops.get(i+1);
					test.clear();
					test.addAll(items);
					for(StockItemTransaction tr : prev.getWorkOrder().getRequiredTransaction()){
						if(!test.remove(tr.getItem()))
							prevCount ++;
					}
					lowestValue = test.size()+prevCount;
					
					int lowestValueProduced = Integer.MAX_VALUE;
					int lowestIndexProduced = -1;
					itemsProduced.clear();
					for(StockItemTransaction tr : next.getWorkOrder().getProducedTransactionList()){
						if(!itemsProduced.contains(tr.getItem()))
							itemsProduced.add(tr.getItem());
					}
					
					test.clear();
					test.addAll(itemsProduced);
					prevCount = 0;
					for(StockItemTransaction tr : prev.getWorkOrder().getProducedTransactionList()){
						if(!test.remove(tr.getItem()))
							prevCount++;
					}
					
					lowestValueProduced = test.size() + prevCount;
					
					//while we are at it try to put operations closer together
					//and resolve overlaps
					if(prev.getEndDate() != next.getStartDate()){
						mNextMoves.add( new MoveBefore(prev, next) );
						//mNextMoves.add( new CustomChangeMove(prev, new Long(next.getStartDate() - prev.getDuration())) );
						//return mNextMoves.size();
					}
					
					
					int j;
					for(j = 0; j < rops.size(); j++){
						if(j == i)
							continue;
						prev = rops.get(j);
						
						if(maxStartTime > 0 && prev.getStartDate() > maxStartTime)
							continue;
						if(minStartTime > 0 && prev.getStartDate() < minStartTime)
							continue;
						
						
						test.clear();
						test.addAll(items);
						prevCount = 0;
						for(StockItemTransaction tr : prev.getWorkOrder().getRequiredTransaction()){
							if(!test.remove(tr.getItem()))
								prevCount++;
						}
						int val = test.size() + prevCount;
						if(val < lowestValue){
							lowestValue = val;
							lowestIndex = j;
							//long moveTo;
							if( j <= i){
								mNextMoves.add( new MoveBefore(next, prev) );
								//moveTo = prev.getEndDate() - next.getDuration(); //moving to front								
							}else{
								mNextMoves.add( new MoveAfter(next, prev) );
								//moveTo = prev.getEndDate();	//backwards
							}
							//if(next.getStartDate() != moveTo){
								//mNextMoves.add( new CustomChangeMove(next, moveTo) );
								//return mNextMoves.size();
							//}
						}
						
						test.clear();
						test.addAll(itemsProduced);
						prevCount = 0;
						for(StockItemTransaction tr : prev.getWorkOrder().getProducedTransactionList()){
							if(!test.remove(tr.getItem()))
								prevCount++;
						}
						
						val = test.size() + prevCount;
						if(val < lowestValueProduced){
							lowestValueProduced = val;
							lowestIndexProduced = j;
							//long moveTo;
							if( j <= i){
								mNextMoves.add( new MoveBefore(next, prev) );
								//moveTo = prev.getEndDate() - next.getDuration(); //moving to front								
							}else{
								mNextMoves.add( new MoveAfter(next, prev) );
								//moveTo = prev.getEndDate();	//backwards
							}
							//if(next.getStartDate() != moveTo){
							//	mNextMoves.add( new CustomChangeMove(next, moveTo) );
								//return mNextMoves.size();
							//}
						}
						
						//if(lowestValue <= 0)
						//	break;
					}
					
					
				}
				
			}
			
			//minimize overlaps
			
			
			
			return mNextMoves.size();
			
		}
		
		
		
	}
	
	public CustomChangeMoveFactory(){
		super();
	}
	
	
	@Override
	public Iterator<Move> createOriginalMoveIterator(ScoreDirector scoreDirector) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Move> createRandomMoveIterator(ScoreDirector scoreDirector, Random r) {
		Schedule s = (Schedule) scoreDirector.getWorkingSolution();
		
		Iterator<Move> it = new MoveIterator(s, r);
		
		return it;
	}

	@Override
	public long getSize(ScoreDirector scoreDirector) {
		Schedule s = (Schedule) scoreDirector.getWorkingSolution();
		return s.getOperationList().size();
	}

}
