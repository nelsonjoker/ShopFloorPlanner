package com.joker.planner.solver.filter;

import org.optaplanner.core.impl.heuristic.selector.common.decorator.SelectionFilter;
import org.optaplanner.core.impl.heuristic.selector.move.generic.SwapMove;
import org.optaplanner.core.impl.score.director.ScoreDirector;

import com.joker.planner.domain.Operation;

public class OperationSwapMoveFilter implements SelectionFilter<SwapMove>{

	@Override
	public boolean accept(ScoreDirector scoreDirector, SwapMove move) {
		
		Operation left = (Operation) move.getLeftEntity();
		Operation right = (Operation) move.getRightEntity();
		
		return left.getResource().equals(right.getResource());
	}

}
