package pt.sotubo.planner.solver;

import java.util.List;
import org.optaplanner.core.impl.domain.variable.listener.VariableListener;
import org.optaplanner.core.impl.score.director.ScoreDirector;

import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.WorkOrder;

/**
 * Keep the workorder start and end time in sync
 * @author Nelson
 */
public class OperationDateUpdatingVariableListener implements VariableListener<Operation> {

    public void beforeEntityAdded(ScoreDirector scoreDirector, Operation operation) {
        // Do nothing
    }

    public void afterEntityAdded(ScoreDirector scoreDirector, Operation operation) {
        updateOperation(scoreDirector, operation);
    }

    public void beforeVariableChanged(ScoreDirector scoreDirector, Operation operation) {
        // Do nothing
    }

    public void afterVariableChanged(ScoreDirector scoreDirector, Operation operation) {
    	updateOperation(scoreDirector, operation);
    }

    public void beforeEntityRemoved(ScoreDirector scoreDirector, Operation operation) {
        // Do nothing
    }

    public void afterEntityRemoved(ScoreDirector scoreDirector, Operation operation) {
        // Do nothing
    }

    
    protected void updateOperation(ScoreDirector scoreDirector, Operation originalOperation) {
    	
    	//System.out.println("updateOperation");
    	
    	WorkOrder wo = originalOperation.getWorkOrder();
    	List<Operation> woOps = wo.getOperations();
    	Long firstStart = null;
    	Long lastEnd = null;
    	for(Operation o : woOps){
    		Long s = o.getStartDate();
    		Long e = o.getEndDate();
    		if(firstStart == null || s < firstStart)
    			firstStart = s;
    		if(lastEnd == null || e > lastEnd)
    			lastEnd = e;
    	}
    	
    	if(!IsDateEqual(firstStart, wo.getFirstStart()) || !IsDateEqual(lastEnd, wo.getLastEnd()))
    	{
	    	scoreDirector.beforeVariableChanged(wo, "firstStart");
	    	wo.setFirstStart(firstStart);
	    	wo.setLastEnd(lastEnd);
	    	scoreDirector.afterVariableChanged(wo, "firstStart");
    	}
    	
    }
    
    protected boolean IsDateEqual(Long a, Long b){
    	if(a == null && b == null)
    		return true;
    	if(a == null || b == null)
    		return false;
    	return a.equals(b);
    }
    

}
