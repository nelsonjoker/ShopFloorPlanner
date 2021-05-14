package pt.sotubo.planner.solver.filter;

import org.optaplanner.core.impl.heuristic.selector.common.decorator.SelectionFilter;
import org.optaplanner.core.impl.heuristic.selector.move.generic.ChangeMove;
import org.optaplanner.core.impl.score.director.ScoreDirector;

import pt.sotubo.planner.domain.Operation;

public class LastOperationChangeMoveFilter implements SelectionFilter<ChangeMove>{

	@Override
	public boolean accept(ScoreDirector scoreDirector, ChangeMove move) {
		
		Operation op = (Operation) move.getEntity();
		long target = (long) move.getToPlanningValue();
		Long end = op.getWorkOrder().getExecutionGroupForcedEnd();
		if(end != null && target > end){
			return false;
		}
		
		end = op.getWorkOrder().getLastEnd();
		if(target > end){
			return false;
		}
		
		/*
		for(ExecutionGroup g : op.getWorkOrder().getExecutionGroups()){
			Long end = g.getEndTime();
			if(end != null && end > 0){
				if(target >= end){
					return false;
				}
			}
		}
		*/
		
		return true;
	}

}
