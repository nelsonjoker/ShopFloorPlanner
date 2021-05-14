package pt.sotubo.planner.solver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Schedule;

public abstract class BaseSolver {
	
	private List<NewBestSolutionListener> mSolutionListeners;
	protected Lock mWorkingSolutionSync;
	protected Schedule mWorkingSolution;
	protected volatile boolean mWorkingSolutionChanged;
	
	public interface NewBestSolutionListener{
		void OnNewBestSolution(BaseSolver solver, Score score);
	}
	
	public BaseSolver(Schedule workingSolution) {
		mSolutionListeners = new ArrayList<>();
		mWorkingSolutionSync = new ReentrantLock(true);
		mWorkingSolution = workingSolution;
		mWorkingSolutionChanged = true;
	}

	public synchronized void addNewBestSolutionListener(NewBestSolutionListener listener){
		mSolutionListeners.add(listener);
	}
	
	protected void OnNewBestSolution(Score score){
		for(NewBestSolutionListener l : mSolutionListeners){
			l.OnNewBestSolution(this, score);
		}
	}
	
	
	/**
	 * start the solving
	 */
	public abstract void solve();

	
	/**
	 * make solve() return now and stop any solving threads
	 */
	public abstract void terminateEarly();

	/**
	 * return a clone of current solution
	 * for thread safety purpose
	 * @return
	 */
	public abstract Schedule getBestScheduleClone();
	
	/**
	 * direct adjust on sequence for operation
	 * @param o
	 * @param t
	 * @return
	 */
	public abstract void doMove(Operation o, long t);
	
	public Schedule lockWorkingSolution(){
		mWorkingSolutionSync.lock();
		return mWorkingSolution;
	}

	public void releaseWorkingSolution(){
		releaseWorkingSolution(mWorkingSolutionChanged);
	}
	
	public void releaseWorkingSolution(boolean changed){
		//only set here, if another one comes along it has no
		//effect only main cycle can reset it
		if(changed)
			mWorkingSolutionChanged = true;
		mWorkingSolutionSync.unlock();
	}

	

}
