package pt.sotubo.planner.solver;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.optaplanner.core.impl.heuristic.move.Move;
import org.optaplanner.core.impl.heuristic.selector.common.decorator.SelectionProbabilityWeightFactory;
import org.optaplanner.core.impl.heuristic.selector.move.generic.ChangeMove;
import org.optaplanner.core.impl.score.director.ScoreDirector;

import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Schedule;
import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemTransaction;

public class ChangeProbabilityWeightFactoryClass implements SelectionProbabilityWeightFactory<ChangeMove> {

	
	
	
	@Override
	public double createProbabilityWeight(ScoreDirector paramScoreDirector, ChangeMove mv) {
		
		//the most late has the most probability
		double p = 0;
		
		Long min = Long.MAX_VALUE;
		Long max = Long.MIN_VALUE;
		Operation prev = null;
		Operation next = null;
		
		Schedule sol = (Schedule) paramScoreDirector.getWorkingSolution();
		List<Operation> ops = sol.getOperationList();
		Operation target = (Operation) mv.getEntity();
		long targetT = (long) mv.getToPlanningValue();
		long tartEnd = targetT + target.getDuration();
		for(Operation o : ops){
			
			if(o.getStartDate() < min)
				min = o.getStartDate();
			if(o.getEndDate() > max)
				max = o.getEndDate();
			if(target.getResource().equals(o.getResource())){
				if(o.getEndDate() < targetT){
					if(prev == null || o.getEndDate() > prev.getEndDate())
						prev = o;
				}
				if(o.getStartDate() > tartEnd){
					if(next == null || o.getStartDate() < next.getStartDate())
						next = o;
				}
			}
			
		}
		
		p = (Math.max(targetT, min) - min)/(double)(max - min);
		
		//count the number of component diffs
		//List<StockItem> diff = new ArrayList<StockItem>(10);
		List<StockItemTransaction> reqs = target.getWorkOrder().getRequiredTransaction(); 
		List<StockItem> items = new ArrayList<StockItem>(reqs.size());
		for(StockItemTransaction t : reqs)
			items.add(t.getItem());
		
		
		double total = items.size();
		if(total > 0){
			double miss = 0;
			if(prev != null){
				miss = missing(items, prev);
				p *= 1-(miss / total);
			}
			if(next != null){
				miss = missing(items, next);
				p *= 1-(miss / total);
			}
		}
		return 0.5+0.5*p;
	}

	public int missing(List<StockItem> items, Operation other){
		
		List<StockItem> reqs = new ArrayList<StockItem>(items);
		
		for(StockItemTransaction t : other.getWorkOrder().getRequiredTransaction()){
			reqs.remove(t.getItem());
		}
		
		
		return reqs.size();
	}
	
	
}
