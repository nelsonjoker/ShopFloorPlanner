package com.joker.planner.solver.filter;

import org.optaplanner.core.impl.heuristic.selector.common.decorator.SelectionFilter;
import org.optaplanner.core.impl.score.director.ScoreDirector;

import com.joker.planner.domain.Operation;


/**
 * Make the allocations that are currently being executed
 * or are closed not movable...
 * @author Nelson
 */
public class MovableOperationFilter implements SelectionFilter<Operation> {

    public boolean accept(ScoreDirector scoreDirector, Operation op) {
    	//Schedule sch = (Schedule) scoreDirector.getWorkingSolution();
    	return op.isMovable();
    	/*
        ShiftDate shiftDate = shiftAssignment.getShift().getShiftDate();
        NurseRoster nurseRoster = (NurseRoster) scoreDirector.getWorkingSolution();
        return nurseRoster.getNurseRosterInfo().isInPlanningWindow(shiftDate);
        */
    }
    
    /*
    public boolean accept(ScoreDirector scoreDirector, Allocation allocation) {
        JobType jobType = allocation.getJob().getJobType();
        return jobType != JobType.SOURCE && jobType != JobType.SINK;
    }
    */

}