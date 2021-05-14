package pt.sotubo.planner.solver;

import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Schedule;

/**
 * A no op solver
 * @author Nelson
 *
 */
public class NoSolver extends BaseSolver {

	private Boolean mRunning;
	
	public NoSolver(Schedule schedule) {
		super(schedule);
		mRunning = false;
	}
	
	

	@Override
	public void solve() {
		
		mRunning = true;
		Score dummyScore = Score.valueOf(new long[3], new long[6]);
		mWorkingSolution.setScore(dummyScore);
		while(mRunning){
			lockWorkingSolution();
			
			if(mWorkingSolutionChanged){
				mWorkingSolutionChanged = false;
				
				OnNewBestSolution(dummyScore);
			}
			
			
			releaseWorkingSolution();
			try {
				synchronized (mRunning) {
					mRunning.wait(5000);
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void terminateEarly() {
		mRunning = false;
		synchronized (mRunning) {
			mRunning.notifyAll();
		}
	}

	@Override
	public Schedule getBestScheduleClone() {
		Schedule cl = null;
		if(mWorkingSolution != null){
			cl = mWorkingSolution.planningClone();
		}
		return cl;
	}

	@Override
	public void doMove(Operation o, long t) {
		//no point in doing anything
	}

	
}
