package pt.sotubo.planner.solver;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import org.optaplanner.core.impl.phase.custom.AbstractCustomPhaseCommand;
import org.optaplanner.core.impl.score.director.ScoreDirector;

import pt.sotubo.planner.SortedArrayList;
import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Resource;
import pt.sotubo.planner.domain.Schedule;
import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemProductionTransaction;
import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;

public class ScheduleSolutionInitializer extends AbstractCustomPhaseCommand {
	
	private long delayOnRequirementsChange;
	
	public ScheduleSolutionInitializer(){
		delayOnRequirementsChange = 60;
	}

	
	
	 public void applyCustomProperties(Map<String, String> customPropertyMap) {
        String delayOnRequirementsChangeString = customPropertyMap.get("delayOnRequirementsChange");
        if (delayOnRequirementsChangeString != null) {
        	delayOnRequirementsChange = Long.parseLong(delayOnRequirementsChangeString);
        }
    }
	
	 private int diff(List<StockItemTransaction> l1, List<StockItemTransaction> l2){
		 
		 Set<StockItem> test = new TreeSet<StockItem>();
		 for(StockItemTransaction tr : l1){
			 test.add(tr.getItem());
		 }
		 int remaining = 0;
		 for(StockItemTransaction tr: l2){
			 if(!test.remove(tr.getItem()))
				 remaining++;
		 }
			 
		 
		 return test.size() + remaining;
	 }

	public void test(ScoreDirector scoreDirector) {
		Schedule s =  (Schedule) scoreDirector.getWorkingSolution();
		List<Operation> ops = s.getOperationList();
		
		Map<List<StockItemTransaction>, List<Operation>> operationGroups = new HashMap<List<StockItemTransaction>, List<Operation>>();
		
		for(Operation o : ops){
			List<StockItemTransaction> matchedRequirements = null;
			for(List<StockItemTransaction> l : operationGroups.keySet()){
				if(diff(l, o.getWorkOrder().getRequiredTransaction()) == 0){
					matchedRequirements = l;
					break;
				}
			}
			if(matchedRequirements == null){
				matchedRequirements = new ArrayList<StockItemTransaction>(o.getWorkOrder().getRequiredTransaction());
				operationGroups.put(matchedRequirements, new ArrayList<Operation>());
			}
			operationGroups.get(matchedRequirements).add(o);
		}
		int totalOps = ops.size();
		int totalGroups = operationGroups.size();
		
		
		
		
		
	}
	
	
	private Map<Operation, Set<Long>> lastMoves = new HashMap<Operation, Set<Long>>();
	
	protected boolean acceptMove(Operation o, Long target){
		Set<Long> os = lastMoves.get(o);
		if(os == null){
			os = new HashSet<Long>(100);
			lastMoves.put(o, os);
		}
		
		if(os.size() > 1000)
			os.remove(os.iterator().next());
		
		return os.add(target);
		
	}
	
	@Override
	public void changeWorkingSolution(ScoreDirector scoreDirector) {
		//test(scoreDirector);
		Schedule s =  (Schedule) scoreDirector.getWorkingSolution();
		List<Operation> ops = s.getOperationList();
		Map<String, List<WorkOrder>> invoiceMap = new HashMap<String, List<WorkOrder>>();
		List<WorkOrder> allWo = s.getWorkOrderList();
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
		
		List<Resource> res = s.getResourceList();
		Map<Resource, List<Operation>> resourceOperationMap = new HashMap<Resource, List<Operation>>();
		for(Resource r : res){
			resourceOperationMap.put(r, new ArrayList<Operation>());
		}
		for(Operation o : ops){
			resourceOperationMap.get(o.getResource()).add(o);
		}
		
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
    	
    	
		do{
			
			System.out.println("Startting iteration " + iteration + " -------------------------------- ");
			
			mod = false;
			dbgCounter = 0;
			
			for(Operation o : ops){
				WorkOrder wo = o.getWorkOrder();
				Long end = wo.getExecutionGroupForcedEnd();
				if(end != null && o.getEndDate() > end){
					scoreDirector.beforeVariableChanged(o, "startDate");
					o.setStartDate(end - o.getDuration());
					scoreDirector.afterVariableChanged(o, "startDate");
					mod = true;
					dbgCounter++;
				}
			}
			System.out.println(iteration + " Initialized "+dbgCounter+" items on this execution group end pass");
			
			
			dbgCounter = 0;
			for(Resource r : res){
				List<Operation> rops = resourceOperationMap.get(r);
				if(rops.size() < 2)
					continue;
				//set all ops to non overlapping 5 h per day max
				sorted.clear();
				sorted.addAll(rops);
				long dayT = sorted.get(sorted.size() - 1).getStartDate()/(24*3600);
				dayT *= 24*3600;
				long hour = 17*3600; // set to end at 5 PM
				for(int i = sorted.size() - 1; i >=0 ; i--){
					Operation o = sorted.get(i);
					long newStart = dayT + hour - o.getDuration();
					if((newStart < o.getStartDate()) && acceptMove(o, newStart)){
						scoreDirector.beforeVariableChanged(o, "startDate");
						o.setStartDate(newStart);
						scoreDirector.afterVariableChanged(o, "startDate");
						mod = true;
						dbgCounter ++;
						hour = newStart - dayT;
					}else{
						hour = (o.getStartDate() % (24*3600)) - o.getDuration();
					}
					
					
					if(hour <= 10*3600){
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
			
			System.out.println(iteration + " Initialized "+dbgCounter+" items on this capacity normalization pass");
			
			
			dbgCounter = 0;
			
			
			for(String vcr : invoiceMap.keySet()){
				List<WorkOrder> woList = invoiceMap.get(vcr);
				for(WorkOrder wo : woList){
					if(wo.getProducedTransactionList().size() == 0)
						continue;
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
					for(WorkOrder c : consumers){
						if(c.getOperations().isEmpty())
							continue;
						for(Operation o : c.getOperations()){
							if(o.getStartDate() < firstConsumer){
								firstConsumer = o.getStartDate();
								wo.setNextOrder(c);
							}
						}
					}
					//second step, place all operations behind any forced end group
					Long minEnd =  wo.getExecutionGroupForcedEnd();
					if(minEnd != null && minEnd < firstConsumer)
						firstConsumer = minEnd;

					
					if(firstConsumer < Long.MAX_VALUE){
						for(Operation o : wo.getOperations()){
							Long newStart = firstConsumer - o.getDuration();
							if(o.isMovable() && o.getEndDate() > firstConsumer && acceptMove(o, newStart)){
								scoreDirector.beforeVariableChanged(o, "startDate");
								o.setStartDate(newStart);
								scoreDirector.afterVariableChanged(o, "startDate");
								dbgCounter++;								
								mod = true;
							}
							
						}
					}
					
					
				}
				
				
			}
			System.out.println(iteration + " Initialized "+dbgCounter+" items on this stock normalization pass");
			
			dbgCounter = 0;
			for(Resource r : res){
				List<Operation> rops = resourceOperationMap.get(r);
				if(rops.size() < 2)
					continue;
				//set all ops to non overlapping 5 h per day max
				sorted.clear();
				sorted.addAll(rops);
				
				for(int i = 0; i < sorted.size()-1; i++){
					
					List<StockItem> consumedPrev = new ArrayList<StockItem>(10);
					WorkOrder wo = sorted.get(i).getWorkOrder();
					for(StockItemTransaction tr : wo.getRequiredTransaction()){
						consumedPrev.add(tr.getItem());
					}
					
					Long maxEnd = wo.getExecutionGroupForcedEnd();
					
					int bestIndex = -1;
					int bestValue = 0;
					for(int j = i + 1 ; j < sorted.size(); j++){
						Operation o  = sorted.get(j);
						if(maxEnd != null && o.getStartDate() >= maxEnd){
							break;
						}
						wo = o.getWorkOrder();
						int val = 0;
						for(StockItemTransaction tr : wo.getRequiredTransaction()){
							StockItem it = tr.getItem();
							if(consumedPrev.contains(it))
								val++;
						}
						if(val > bestValue){
							bestValue = val;
							bestIndex = j;
						}					
					}
					if(bestIndex > i+1){
						//there is a better one up ahead
						Operation o = sorted.get(i);
						Operation tgt = sorted.get(bestIndex);
						Long newStart = tgt.getStartDate() - o.getDuration();
						if(o.isMovable() && o.getEndDate() < tgt.getStartDate() && acceptMove(o, newStart)){
							scoreDirector.beforeVariableChanged(o, "startDate");
							o.setStartDate(newStart);
							scoreDirector.afterVariableChanged(o, "startDate");
							dbgCounter++;								
							mod = true;
						}
						
					}
					
					
				}
				
				
				
				
			}
			System.out.println(iteration + " Initialized "+dbgCounter+" items on this consumption normalization pass");
			
			//now try to schedule operations per resource in non overlapping fashion
			//maintaining order
			/*
			ops.sort(new Comparator<Operation>() {
				@Override
				public int compare(Operation o1, Operation o2) {
					return (int) (o2.getEndDate() - o1.getEndDate());	//reverse order
				}
			});
			
			List<Resource> res = s.getResourceList();
			Map<Resource, List<Operation>> resourceOperationMap = new HashMap<Resource, List<Operation>>();
			for(Resource r : res){
				resourceOperationMap.put(r, new ArrayList<Operation>());
			}
			for(Operation o : ops){
				resourceOperationMap.get(o.getResource()).add(o);
			}
			List<StockItem> test = new ArrayList<StockItem>();
			for(Resource r : res){
				Long lastStart = null;
				Operation lastOp = null;
				List<Operation> rops = resourceOperationMap.get(r);
				test.clear();
				for(Operation o : rops){
					if(lastStart != null){
						
						//add an interval if items changed
						for(StockItemTransaction tr : o.getWorkOrder().getRequiredTransaction()){
							test.remove(tr.getItem());
						}
						if(test.size() > 0)
							lastStart -= delayOnRequirementsChange;

						
						if(o.getEndDate() > lastStart){
							scoreDirector.beforeVariableChanged(o, "startDate");
							o.setStartDate(lastStart - o.getDuration());
							scoreDirector.afterVariableChanged(o, "startDate");
							mod = true;
						}
					}
					lastOp = o;
					lastStart = o.getStartDate();
					test.clear();
					for(StockItemTransaction tr : o.getWorkOrder().getRequiredTransaction()){
						if(!test.contains(tr.getItem()))
							test.add(tr.getItem());
					}
				}
				
				
			}
			*/
			
			
			
			
			
			iteration++;
		}while(mod);
		
		scoreDirector.triggerVariableListeners();
		
		
		
	}

}
