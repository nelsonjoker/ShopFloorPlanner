package pt.sotubo.planner;

import java.util.Collection;

import org.optaplanner.core.api.domain.solution.Solution;
import org.optaplanner.core.api.score.Score;
import org.optaplanner.core.api.score.constraint.ConstraintMatchTotal;
import org.optaplanner.core.impl.domain.variable.descriptor.VariableDescriptor;
import org.optaplanner.core.impl.score.director.ScoreDirector;

public class NullScoreDirector implements ScoreDirector {

	private Solution mSolution;
	
	public NullScoreDirector(Solution workingSolution){
		mSolution = workingSolution;
	}
	
	@Override
	public Solution getWorkingSolution() {
		return mSolution;
	}

	@Override
	public void setWorkingSolution(Solution workingSolution) {
		mSolution = workingSolution;
	}

	@Override
	public Score calculateScore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isConstraintMatchEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Collection<ConstraintMatchTotal> getConstraintMatchTotals() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void beforeEntityAdded(Object entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterEntityAdded(Object entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeVariableChanged(Object entity, String variableName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterVariableChanged(Object entity, String variableName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeVariableChanged(VariableDescriptor variableDescriptor, Object entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterVariableChanged(VariableDescriptor variableDescriptor, Object entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void changeVariableFacade(VariableDescriptor variableDescriptor, Object entity, Object newValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void triggerVariableListeners() {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeEntityRemoved(Object entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterEntityRemoved(Object entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeProblemFactAdded(Object problemFact) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterProblemFactAdded(Object problemFact) {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeProblemFactChanged(Object problemFact) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterProblemFactChanged(Object problemFact) {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeProblemFactRemoved(Object problemFact) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterProblemFactRemoved(Object problemFact) {
		// TODO Auto-generated method stub

	}

	@Override
	public void dispose() {
		// TODO Auto-generated method stub

	}

}
