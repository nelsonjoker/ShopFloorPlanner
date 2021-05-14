package pt.sotubo.planner.solver.filter;

import java.util.ArrayList;
import java.util.List;

import org.optaplanner.core.impl.heuristic.selector.common.decorator.SelectionFilter;
import org.optaplanner.core.impl.heuristic.selector.move.generic.ChangeMove;
import org.optaplanner.core.impl.score.director.ScoreDirector;

import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Schedule;
import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemTransaction;

public class MatchRequirementsChangeMoveFilter extends LastOperationChangeMoveFilter{

	@Override
	public boolean accept(ScoreDirector scoreDirector, ChangeMove move) {
		
		if(!super.accept(scoreDirector, move))
			return false;
		
		Operation op = (Operation) move.getEntity();
		List<StockItemTransaction> reqs = op.getWorkOrder().getRequiredTransaction();
		Long targetTime = (Long)move.getToPlanningValue();
		
		Schedule sch = (Schedule) scoreDirector.getWorkingSolution();
		
		List<StockItemTransaction> preqs = null;
		
		Long minDistance = null;
		for(Operation o : sch.getOperationList()){
			if(o.equals(op))
				continue;
			if(o.getResource().equals(op.getResource())){
				if(o.getEndDate() < targetTime){
					if(minDistance == null || targetTime - o.getEndDate() < minDistance){
						minDistance = targetTime - o.getEndDate();
						preqs = o.getWorkOrder().getRequiredTransaction();						
					}
				}
			}
		}
		
		if(preqs != null){
			int total = reqs.size();
			List<StockItem> items = new ArrayList<StockItem>(total);
			for(StockItemTransaction r : reqs){
				items.add(r.getItem());
			}
			
			for(StockItemTransaction r : preqs){
				items.remove(r.getItem());
			}
			if(items.isEmpty())
				return true;
		}

		
		return false;
	}

}