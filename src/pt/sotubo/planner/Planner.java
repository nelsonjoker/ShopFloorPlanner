package pt.sotubo.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.time.OffsetDateTime;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SerializationUtils;
import org.optaplanner.core.api.score.Score;
import org.optaplanner.core.api.score.buildin.bendablelong.BendableLongScore;
import org.optaplanner.core.api.solver.Solver;
import org.optaplanner.core.api.solver.SolverFactory;
import org.optaplanner.core.api.solver.event.BestSolutionChangedEvent;
import org.optaplanner.core.api.solver.event.SolverEventListener;
import org.optaplanner.core.impl.score.director.ScoreDirector;
import org.optaplanner.core.impl.solver.ProblemFactChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;

import pt.sotubo.planner.NaiveSolver.NewBestSolutionListener;
import pt.sotubo.planner.domain.ExecutionGroup;
import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Resource;
import pt.sotubo.planner.domain.Schedule;
import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemProductionTransaction;
import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;
import pt.sotubo.planner.solver.OperationDateUpdatingVariableListener;


public class Planner implements Runnable{

	//public static final String SOLVER_CONFIG = "pt/sotubo/planner/projectJobSchedulingSolverConfig.xml";
	public static final String SOLVER_CONFIG = "projectJobSchedulingSolverConfig.xml";
	 /**
     * offset from current time for planning window
     */
	public static final int PLANNING_WINDOW_OFFSET = 4*3600;
    protected transient Logger logger = null;
    private Object mWaitHandle;
    private Object mWaitProblemFactChange;
    private Solver<Schedule> mSolver;
    private int mExecutionGroupCounter;
    private int mWorkorderCounter;
    
    private boolean mCancellationPending;
    protected boolean isCancellationPending(){ return mCancellationPending; }
    
    protected Logger getLogger() { return logger; }
    
    private String mPlannerUUID;
    protected String getPlannerUUID() { return mPlannerUUID; }
    
    private int mVersionCode = 1;
    public int getVersionCode() { return mVersionCode; }
    
    private int mSolverBackoff;
    
    
    private Object mFeedSync = new Object();	//sync on the feed workers
    private Cursor<HashMap> mExecutionGroupsCursor;
    private Cursor<HashMap> mWorkordersCursor;
    private Cursor<HashMap> mItemStockCursor;
    private Cursor<HashMap> mPlanCursor;
    
    private long mLastChangeTime;
    private Schedule mLastBestSolution;
    private ArrayBlockingQueue<String> mPlansToDelete;
    private ArrayBlockingQueue<String> mWorkordersToClose;
    
    public Planner(){
    	System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    	logger =  LoggerFactory.getLogger(Planner.class);
    	SolverFactory<Schedule> solverFactory = SolverFactory.createFromXmlResource(SOLVER_CONFIG);
        mSolver = solverFactory.buildSolver();
        mWaitHandle = new Object();
        mWaitProblemFactChange = new Object();
        mCancellationPending = false;
        mPlannerUUID = DbRethinkMap.UUID();
        
         try {
			InetAddress add = getLocalHostLANAddress();
			if(add != null)
				mPlannerUUID = add.getHostName()+"@"+add.getHostAddress()+"/"+System.currentTimeMillis();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        mSolverBackoff = 0;
        mLastChangeTime =  0;
        mLastBestSolution = null;
        mPlansToDelete = new ArrayBlockingQueue<String>(20000);
        mWorkordersToClose = new ArrayBlockingQueue<String>(20000);
        
    }
    
    
    protected void Cancel() {
    	mCancellationPending = true;
    	synchronized (mWaitProblemFactChange) {
    		mWaitProblemFactChange.notifyAll();
    	}
    	synchronized (mWaitHandle) {
    		mWaitHandle.notifyAll();
    	}
    	
    }
    
    protected void SleepWait(long timeout){
    	synchronized (mWaitHandle) {
    		try {
				mWaitHandle.wait(timeout);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }
    
    protected void updateThreads(){
    	Thread deletePlans = new Thread(new Runnable() {
			
			@Override
			public void run() {
				String mfgnum = "";
				try {
					while((mfgnum = mPlansToDelete.take()) != ""){
						try{
							DbRethinkMap.Delete("plan", RethinkDB.r.hashMap("mfgnum", mfgnum));
						}catch(Exception e1){
							e1.printStackTrace();
							SleepWait(1000);
						}
						
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		});
		deletePlans.setName("deletePlans");
		deletePlans.start();
		
		Thread closeWorkOrders = new Thread(new Runnable() {
			
			@Override
			public void run() {
				String mfgnum = "";
				Map<String,  Object> upd = new HashMap<String,Object>();
				upd.put("status", "FIN");
				try {
					while((mfgnum = mWorkordersToClose.take()) != ""){
						try{
							DbRethinkMap.UpdateIf("workorder", mfgnum, upd, "status", "MOD");
						}catch(Exception ex){
							ex.printStackTrace();
							SleepWait(1000);
						}
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		});
		closeWorkOrders.setName("closeWorkOrders");
		closeWorkOrders.start();
    }
    
    private Schedule mSchedule;
	@Override
	public void run() {

		updateThreads();
		
		//debugFixWorkOrders();
		//int res = DbRethinkMap.DeleteIfLT("plan", null, "update", 0);
		/*
		//assume resource won't change
		mSchedule = new Schedule();
		
		getLogger().info("Loading resources...");
		mSchedule.setResourceList(loadResources());
		getLogger().info("Loading execution groups...");
		mExecutionGroupCounter = loadExecutionGroups(mSchedule);
		getLogger().info("Loading work orders...");
		mWorkorderCounter = loadWorkOrders(mSchedule);
		getLogger().info("Loading stock...");
		loadStockItems(mSchedule);
		getLogger().info("Loading previous plans...");
		loadCurrentPlans(mSchedule);
		
		try {
			SerializationUtils.serialize(mSchedule, new FileOutputStream("schedule.data"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		try {
			mSchedule = (Schedule) SerializationUtils.deserialize(new FileInputStream("schedule.data"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		BlockingQueue<Schedule> mBestSolutions = new ArrayBlockingQueue<Schedule>(1);
		getLogger().info("Listening for updates...");
		
		Semaphore mBestScoreAvailable = new Semaphore(0, true);//TODO: not the best solution
		
		NaiveSolver test = new NaiveSolver();
		test.addNewBestSolutionListener(new NewBestSolutionListener() {
			
			private Score mLastScore = null;
			
			@Override
			public void OnNewBestSolution(NaiveSolver solver, Score score) {
				
				if(mLastScore == null || score.compareTo(mLastScore) > 0){
					mBestScoreAvailable.release();
				}
				mLastScore = score;
				//synchronized (mBestSolutions) {
				//	mBestSolutions.notifyAll();							
				//}
			}
		});
		

		Thread solverThread = new Thread(new Runnable() {
			@Override
			public void run() {
				test.run(mSchedule);				
			}
		});
		solverThread.start();
		
		/*
		try {
			solverThread.join();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		
		Thread waitSolutions = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(true){
					try {
						
						mBestScoreAvailable.acquire();
						//synchronized (mBestSolutions) {
						//	mBestSolutions.wait();							
						//}
						
						Schedule sol = test.getBestScheduleClone();
						getLogger().info("New solution available " + sol.getScore().toString());
						mBestSolutions.put(sol);
						
						try {
							synchronized (mBestSolutions) {
								if(!mBestSolutions.isEmpty())
									mBestSolutions.remove();
								mBestSolutions.put(sol);							
							}
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
			}
		});
		waitSolutions.start();
		
		
		
        mSolver.addEventListener(new SolverEventListener<Schedule>() {

			@Override
			public void bestSolutionChanged(BestSolutionChangedEvent<Schedule> event) {
				Schedule sol = event.getNewBestSolution();
				//if (event.isNewBestSolutionInitialized() && sol.getScore().isFeasible()) {
				if (event.isNewBestSolutionInitialized()) {
					getLogger().info("New solution available " + event.getNewBestSolution().getScore().toString());
					
					try {
						synchronized (mBestSolutions) {
							if(!mBestSolutions.isEmpty())
								mBestSolutions.remove();
							mBestSolutions.put(sol);							
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
	            }
			}
		});
        
        
        Thread feedGroups = new Thread(new Runnable() {
			
			@Override
			public void run() {
				int state = 0;
				while(!isCancellationPending()){
					synchronized (mFeedSync) {
						mExecutionGroupsCursor = DbRethinkMap.GetAll("execution_group", mExecutionGroupCounter, null);
					}
					try{
						List<Map<String,Object>> buffer = new ArrayList<Map<String,Object>>(100);
						while(!isCancellationPending()){
							switch(state){
								case	0:
										buffer.add(mExecutionGroupsCursor.next());
										mSolverBackoff = 0;
										state = 1;
										break;
								case	1:
										if(buffer.size() >= 100){
											state = 2;
										}else if(mExecutionGroupsCursor.bufferedSize() > 0){
											state = 0;
										}else{
											SleepWait(1000);
											if(mExecutionGroupsCursor.bufferedSize() > 0)
												state = 0;
											else
												state = 2;
										}
										break;
								case	2:
										List<Map<String,Object>> cpy = new ArrayList<Map<String,Object>>(buffer);
										buffer.clear();
										OnExecutionGroupChanged(cpy);
										mSolverBackoff = 0;
										mExecutionGroupCounter += cpy.size();
										state = 0;
										//state = 3;//FIXME
										break;
								case	3:
										SleepWait(1000);
										break;
							}
						}
						/*
						for(HashMap m : mExecutionGroupsCursor){
							OnExecutionGroupChanged(m);
							mSolverBackoff = 0;
							mExecutionGroupCounter++;
							if(isCancellationPending())
								break;
						}
						*/
					
					}catch(Exception e){
						mExecutionGroupsCursor.close();
						mExecutionGroupsCursor = null;
						SleepWait(5000);						
					}finally{
						if(mExecutionGroupsCursor != null)
							mExecutionGroupsCursor.close();
						state = 0;
					}
				}
			}
		});
        feedGroups.setName("FeedGroups");
        //feedGroups.start();
        
        Thread feedWorkorders = new Thread(new Runnable() {
			
        	@Override
			public void run() {
				while(!isCancellationPending()){
					int state = 0;
					synchronized (mFeedSync) {
						mWorkordersCursor = DbRethinkMap.GetAll("workorder", mWorkorderCounter, RethinkDB.r.hashMap("status", "MOD"));	
					}
					
					try{
						List<Map<String,Object>> buffer = new ArrayList<Map<String,Object>>(100);
						while(!isCancellationPending()){
							switch(state){
								case	0:
										buffer.add(mWorkordersCursor.next());
										mSolverBackoff = 0;
										state = 1;
										break;
								case	1:
										if(buffer.size() >= 100){
											state = 2;
										}else if(mWorkordersCursor.bufferedSize() > 0){
											state = 0;
										}else{
											SleepWait(1000);
											if(mWorkordersCursor.bufferedSize() > 0)
												state = 0;
											else
												state = 2;
										}
										break;
								case	2:
										List<Map<String,Object>> cpy = new ArrayList<Map<String,Object>>(buffer);
										buffer.clear();
										OnWorkOrderChanged(cpy);
										mSolverBackoff = 0;
										mWorkorderCounter += cpy.size();
										state = 0;
										//state = 3;	//FIXME
										break;
								case	3:
										SleepWait(1000);
										break;
							}
						}
						/*
						for(HashMap m : mWorkordersCursor){
							OnWorkOrderChanged(m);
							mSolverBackoff = 0;
							mWorkorderCounter++;
							if(isCancellationPending())
								break;
						}
						*/
						
					}catch(Exception e){
						if(mWorkordersCursor != null)
							mWorkordersCursor.close();
						mWorkordersCursor = null;
						SleepWait(5000);						
					}finally{
						if(mWorkordersCursor != null)
							mWorkordersCursor.close();
						state = 0;
					}
				}
			}
		});
        feedWorkorders.setName("FeedWorkorders");
        //feedWorkorders.start();
        
        
        Thread feedItemStocks = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(!isCancellationPending()){
					int state = 0;
					String[] currentItems = null;
					synchronized (mSchedule.getStockItemList()) {
						List<StockItem> stockItemList = mSchedule.getStockItemList();
						currentItems = new String[stockItemList.size()];
						for(int i = 0; i < stockItemList.size(); i++){
							currentItems[i] = stockItemList.get(i).getReference();
						}
					}
					
					if(currentItems == null || currentItems.length == 0){
						SleepWait(1000);
						continue;
					}
					
					synchronized (mFeedSync) {
						mItemStockCursor = DbRethinkMap.GetAll("itmmaster", 0, currentItems, "itmref");	
					}
					
					try{
						
						List<Map<String,Object>> buffer = new ArrayList<Map<String,Object>>(100);
						while(!isCancellationPending() && mItemStockCursor != null){
							switch(state){
								case	0:
										Map<String,Object> it = mItemStockCursor.next();
										if(it != null){
											buffer.add(it);
											mSolverBackoff = 0;
											state = 1;
										}else{
											mItemStockCursor.close();
											mItemStockCursor = null;
										}
										break;
								case	1:
										if(buffer.size() >= 100){
											state = 2;
										}else if(mItemStockCursor.bufferedSize() > 0){
											state = 0;
										}else{
											SleepWait(1000);
											if(mItemStockCursor.bufferedSize() > 0)
												state = 0;
											else
												state = 2;
										}
										break;
								case	2:
										List<Map<String,Object>> cpy = new ArrayList<Map<String,Object>>(buffer);
										buffer.clear();
										OnStockItemChanged(cpy);
										mSolverBackoff = 0;
										state = 0;
										//state = 3; //FIXME
										break;
								case	3:
										SleepWait(1000);
										break;
							}
						}
						/*
						for(HashMap m : mItemStockCursor){							
							OnStockItemChanged(m);
							mSolverBackoff = 0;
							if(isCancellationPending())
								break;
						}
						*/
						
					}catch(Exception e){
						logger.error(e.getMessage());
						mItemStockCursor.close();
						mItemStockCursor = null;
						SleepWait(5000);						
					}finally{
						if(mItemStockCursor != null)
							mItemStockCursor.close();
						mItemStockCursor = null;
						state = 0;
					}			
				}
			}
			
		});
        feedItemStocks.setName("FeedItemStocks");
        //feedItemStocks.start();
        

        Thread feedPlans = new Thread(new Runnable() {
			
        	@Override
			public void run() {
        		
        		mPlanCursor = null;
        		ArrayList<Map<String,Object>> buffer = new ArrayList<Map<String,Object>>();
        		
        		
        		
				
				try{
					synchronized (mFeedSync) {
						mPlanCursor = DbRethinkMap.GetAll("plan", 0, null);
					}
					while(!isCancellationPending()){
						buffer.clear();
						Map<String, Object> pl = null;
						while( buffer.size() < 1 && (pl = mPlanCursor.next(1)) != null){
							boolean assigned = (boolean) pl.getOrDefault("assigned", false);
							boolean completed = (boolean) pl.getOrDefault("completed", false);
							String plannerId = pl.getOrDefault("planner_id", "").toString();
							String mfgnum = pl.get("mfgnum").toString();
							String operation = pl.get("operation").toString();
							long update = (long)pl.getOrDefault("update", 0L);
							long plupdate = (long)pl.getOrDefault("plupdate", 0L);
							
							if(!mPlannerUUID.equals(plannerId) || update != plupdate)
								buffer.add(pl);
						}
						if(buffer.size() > 0){
							List<Map<String,Object>> cpy = new ArrayList<Map<String,Object>>(buffer);
							buffer.clear();
							OnPlanChanged(cpy);
							mSolverBackoff = 0;
						}else{
							SleepWait(1000);
						}
					}
						
					
				}catch(Exception e){
					e.printStackTrace();
					getLogger().error(e.getMessage());
					//SleepWait(1000);
				}finally{
					if(mPlanCursor != null)
						mPlanCursor.close();
					mPlanCursor = null;
				}
				
			}

		});
        feedPlans.setName("FeedPlans");
        //feedPlans.start();
        
        Thread reseter = new Thread(new Runnable() {
			
			@Override
			public void run() {
				long timeout = 3*60*1000L;
				
				while(!isCancellationPending()){
					
					long start = System.currentTimeMillis();
					synchronized (mWaitProblemFactChange) {
						try {
							mWaitProblemFactChange.wait(timeout);
						} catch (InterruptedException e) {
							e.printStackTrace();
							continue;
						}
					}
					if((System.currentTimeMillis() - start) < timeout){
						continue;
					}
					
					if(isCancellationPending())
						continue;
					/*
					synchronized (mBestSolutions) {
						if(!mBestSolutions.isEmpty())
							continue;
					}
					*/
					
					 mSolver.addProblemFactChange(new ProblemFactChange() {
			            public void doChange(ScoreDirector scoreDirector) {
			            	synchronized (mWaitProblemFactChange) {
			            		mWaitProblemFactChange.notifyAll();
			            	}
			            	scoreDirector.triggerVariableListeners();
			            	
			            }
			        });
					
					
				}
				
			}
		});
        reseter.setName("reseter");
        //reseter.start();
        
        Thread t = new Thread(new Runnable() {
			
			@Override
			public void run() {
				getLogger().info("Holding back the solving untill data stable...");
				mSolverBackoff = 0;
				while(mSolverBackoff++ < 10){
					SleepWait(1000);
					if(isCancellationPending())
						return;
				}

				while(!isCancellationPending() && (mSchedule.getWorkOrderList().size() == 0) && mWorkorderCounter == 0){
					getLogger().info("No data to compute, backoff a minute...");	
					SleepWait(60000);
				}
				
				if(!isCancellationPending()){
					getLogger().info("Solving started...");
					mSolver.solve(mSchedule);
				}
			}
		});
        t.setName("solver");
        //t.start();
		
        
        
		
		while(!isCancellationPending()){	
			
			try{
				if(mBestSolutions.isEmpty())
	        		SleepWait(1000);
	        	else{
	        		Schedule last = null;
	        		synchronized (mBestSolutions) {
	        			
		        		while(!mBestSolutions.isEmpty()){
		        			try {
								last = mBestSolutions.take();
							} catch (InterruptedException e) {
								e.printStackTrace();
								continue;
							}
		        		}
		        		
					}
	        		if(last != null){
	        			
	        			synchronized (mBestSolutions) {
	        				mLastBestSolution = last;
	        			}
	        			
	        			//publish our current solution
	        			//- gain exclusive access
	        			//- update operation start times
	        			
	        			//OffsetDateTime now = OffsetDateTime.now(ZoneId.of("UTC"));
	        			long now = System.currentTimeMillis();
	        			
	        			DbRethinkMap lock = new DbRethinkMap();
	        			lock.put("planner", getPlannerUUID());
	        			lock.put("score", last.getScore().toString());
	        			lock.put("version", getVersionCode());
	        			lock.put("time", RethinkDB.r.now());
	        			lock.put("last", mLastChangeTime);
	        			lock.put("status", 1);
	        			
	        			int exStatus = 1;
	        			boolean ignore = false;
	        			int retries = 0;
	        			long top_time = 0;
	        			while(exStatus == 1){
		        			List<DbRethinkMap> existent = DbRethinkMap.GetAll("lock", 0, 1000);
		        			ignore = false;
		        			exStatus = 0;
		        			long batch_top_time = 0;
		        			for(Map<String, Object> e : existent){
		        				
		        				BendableLongScore other = BendableLongScore.parseScore(3, 6, (String) e.get("score"));
		        				int oVersion = Integer.parseInt(e.get("version").toString());
		        				int status = e.containsKey("status") ? Integer.parseInt(e.get("status").toString()) : 0;
		        				long time = ((OffsetDateTime)e.get("time")).toEpochSecond();
		        				if(time > batch_top_time)
		        					batch_top_time = time;
		        				if(status == 1 && retries < 60){
		        					exStatus = 1;
		        				}else{
		        				
			        				boolean del = false;
			        				if(getPlannerUUID().equals(e.get("planner"))){
			        					del = true;
			        				}else if(last.getScore().compareTo(other) > 0){
			        					del = true;
			        				}else if(getVersionCode() > oVersion){
			        					del = true;
			        				}else if(!e.containsKey("last") || mLastChangeTime > (long)e.get("last") ){
			        					del = true;
			        				}
			        				//TODO: need a way to clear old ones..
			        				//else if(now.toEpochSecond() - ((OffsetDateTime)e.get("time")).toEpochSecond() > 3600){
			        				//	del = true;
			        				//}
			        				if(del){
			        					String id = (String)e.get("id");
			        					DbRethinkMap.Delete("lock", id);
			        					//DbRethinkMap.Delete("plan", RethinkDB.r.hashMap("planner_id", id));
			        				}else
			        					ignore = true;
		        				}
		        			}
		        			
		        			
	        				if(exStatus == 1){
			        			if(batch_top_time != top_time){
			        				top_time = batch_top_time;
			        				retries = -1;
		        					getLogger().info("Lock updated, reset retries counter");
			        			}
	        					retries++;
	        					getLogger().info("Concurrent solution update, backing off. retry #"+retries);
	        					SleepWait(60000);
	        				}

	        			}
	        			
	        			if(ignore){
	        				getLogger().info("Solution ignored "+last.getScore());
	        				continue;
	        			}
	        			
	    				getLogger().info("Saving new solution  : "+last.getScore());
	    				
	        			lock.insert("lock");
	        			String planner_id = getPlannerUUID();
	        			
	        			
	        			List<Operation> operations = last.getOperationList(); //new ArrayList<Operation>(); //
	        			List<HashMap<String, Object>> batch_insert = new ArrayList<HashMap<String, Object>>(operations.size());
	        			//List<HashMap<String, Object>> batch_update = new ArrayList<HashMap<String, Object>>(operations.size());
	        			now = System.currentTimeMillis();
	        			long last_lock_update = System.currentTimeMillis();
	        			int counter = 0;
						for(Operation o : operations){
							
							if(counter % 100 == 0){
								getLogger().info("Saved "+counter+" plans of "+operations.size());
							}
							counter++;
							//OffsetDateTime start = OffsetDateTime.ofInstant(Instant.ofEpochSecond(o.getStartDate()), ZoneId.of("UTC"));
							//OffsetDateTime end = OffsetDateTime.ofInstant(Instant.ofEpochSecond(o.getEndDate()), ZoneId.of("UTC"));
							
							if((System.currentTimeMillis() - last_lock_update) > 60000){
								lock.put("time", RethinkDB.r.now());
								lock.update("lock", lock.get("id").toString());
								last_lock_update = System.currentTimeMillis();
							}
							
							
							long start = o.getStartDate()*1000;
							long end = o.getEndDate()*1000;
							String mfgnum = o.getWorkOrder().getMFGNUM();
							HashMap<String, Object> map = DbRethinkMap.Get("plan", RethinkDB.r.hashMap("mfgnum", mfgnum).with("operation", o.getCode()));
							//HashMap<String, Object> map = DbRethinkMap.Get("plan", "mfgnum", mfgnum);
							
							if(map == null){
								//create plan from scratch
								map = new DbRethinkMap();
								map.put("assigned",  false);
								map.put("completed",  false);
								map.put("execution_id",  null);
								map.put("planner_id", planner_id);
								map.put("operation", o.getCode());
								map.put("openum", o.getOPENUM());
								map.put("mfgnum", mfgnum);
								map.put("workcenter", o.getResource().getWorkcenter());
								map.put("workstation", o.getResource().getCode());
								map.put("workstation_number", o.getResource().getInstance());
								map.put("start", start);
								map.put("end",  end);
								map.put("duration",  o.getDuration());
								map.put("update",  now);
								map.put("plupdate",  now);
								map.put("update_counter",  1);
								map.put("plupdate_counter",  1);
								map.put("operations_count",  o.getWorkOrder().getOperations().size());
								map.put("operations_done",  0);
								
								List<DbRethinkMap> produced = new ArrayList<DbRethinkMap>();
								for(StockItemProductionTransaction trans : o.getWorkOrder().getProducedTransactionList()){
									DbRethinkMap prod = new DbRethinkMap();
									prod.put("itmref", trans.getItem().getReference());
									prod.put("quantity", trans.getQuantity());
									prod.put("executed", 0);
									prod.put("vcrnumori", trans.getVCRNUMORI());
									prod.put("stu", trans.getSTU()); 
									
									produced.add(prod);
								}
								map.put("produced",  produced);
								
								
								List<DbRethinkMap> consumed = new ArrayList<DbRethinkMap>();
								for(StockItemTransaction trans : o.getWorkOrder().getRequiredTransaction()){
									DbRethinkMap cons = new DbRethinkMap();
									cons.put("itmref", trans.getItem().getReference());
									cons.put("quantity", trans.getQuantity());
									cons.put("executed", 0);
									cons.put("stu", trans.getSTU());
									consumed.add(cons);
								}
								map.put("consumed",  consumed);
								batch_insert.add(map);
								
							}else{
								
								
								
								boolean success = false;
								while(!success){
									
									HashMap<String, Object> oldMap = map;
									
									if((boolean)oldMap.getOrDefault("assigned", false)){
										break;
									}
								
									String id = (String)oldMap.get("id");
									long update_counter = (Long)oldMap.get("update_counter");
									List<HashMap<String,Object>> consumed = (List<HashMap<String,Object>>) oldMap.get("consumed");
									List<HashMap<String,Object>> produced = (List<HashMap<String,Object>>) oldMap.get("produced");
									
									boolean changed = false;
									
									map = new DbRethinkMap();
									map.put("id", id);
									if(!planner_id.equals(oldMap.get("planner_id"))){
										map.put("planner_id", planner_id);
										changed = true;
									}
									if(!o.getCode().equals(oldMap.get("operation"))){
										map.put("operation", o.getCode());
										changed = true;
									}
									if(o.getOPENUM() != (Long)oldMap.get("openum")){
										map.put("openum", o.getOPENUM());
										changed = true;
									}
									if(!mfgnum.equals(oldMap.get("mfgnum"))){
										map.put("mfgnum", mfgnum);
										changed = true;
									}
									if(!o.getResource().getWorkcenter().equals(oldMap.get("workcenter"))){
										map.put("workcenter", o.getResource().getWorkcenter());
										changed = true;
									}
									if(!o.getResource().getCode().equals(oldMap.get("workstation"))){
										map.put("workstation", o.getResource().getCode());
										changed = true;
									}
									if(o.getResource().getInstance() != (Long)oldMap.get("workstation_number")){
										map.put("workstation_number", o.getResource().getInstance());
										changed = true;
									}
									
									//if(start != ((OffsetDateTime)oldMap.get("start")).toEpochSecond()){
									if(start != ((Long)oldMap.get("start"))){
										map.put("start",  start);
										//map.put("start", RethinkDB.r.epochTime(start));
										changed = true;
									}
									if(end != ((Long)oldMap.get("end"))){
									//if(end != ((OffsetDateTime)oldMap.get("end")).toEpochSecond()){
										map.put("end",  end);
										//map.put("end", RethinkDB.r.epochTime(end));
										changed = true;
									}
									
									if(o.getDuration() != (Long)oldMap.get("duration")){
										map.put("duration",  o.getDuration());
										changed = true;
									}
									
									map.put("update",  now);
									map.put("plupdate",  now);
									map.put("update_counter",  update_counter + 1);
									map.put("plupdate_counter",  update_counter + 1);
									
									
									List<HashMap<String,Object>> filtered = new ArrayList<HashMap<String,Object>>(consumed.size()); 
									boolean list_changed = false;
									for(StockItemTransaction trans : o.getWorkOrder().getRequiredTransaction()){
										String itmref = trans.getItem().getReference();
										float qty = trans.getQuantity();
										String stu = trans.getSTU();
										
										HashMap<String,Object> ex = consumed.stream().filter(m -> itmref.equals((String)m.get("itmref"))).findFirst().orElse(null);
										if(ex == null){
											DbRethinkMap cons = new DbRethinkMap();
											cons.put("itmref", itmref);
											cons.put("quantity", qty);
											cons.put("executed", 0);
											cons.put("stu", stu);
											filtered.add(cons);
											list_changed = true;
										}else{
											if(Math.abs(qty - Float.parseFloat(ex.get("quantity").toString())) > 0.1){
												ex.put("quantity", qty);
												list_changed = true;
											}
											if(!stu.equals(ex.get("stu"))){
												ex.put("stu", stu);
												list_changed = true;
											}
											filtered.add(ex);
										}
									}
									if(list_changed){
										map.put("consumed", filtered);
										changed = true;
									}
									
									
									
									
									filtered = new ArrayList<HashMap<String,Object>>(produced.size()); 
									list_changed = false;
									for(StockItemProductionTransaction trans : o.getWorkOrder().getProducedTransactionList()){
										String itmref = trans.getItem().getReference();
										float qty = trans.getQuantity();
										String stu = trans.getSTU();
										String vcrnumori = trans.getVCRNUMORI();
										
										HashMap<String,Object> ex = produced.stream().filter(m -> itmref.equals((String)m.get("itmref"))).findFirst().orElse(null);
										if(ex == null){
											DbRethinkMap prd = new DbRethinkMap();
											prd.put("itmref", itmref);
											prd.put("quantity", qty);
											prd.put("executed", 0);
											prd.put("stu", stu);
											prd.put("vcrnumori", vcrnumori);
											filtered.add(prd);
											list_changed = true;
										}else{
											if(Math.abs(qty - Float.parseFloat(ex.get("quantity").toString())) > 0.1){
												ex.put("quantity", qty);
												list_changed = true;
											}
											if(!stu.equals(ex.get("stu"))){
												ex.put("stu", stu);
												list_changed = true;
											}
											if(!vcrnumori.equals(ex.get("vcrnumori"))){
												ex.put("vcrnumori", vcrnumori);
												list_changed = true;
											}
											filtered.add(ex);
										}
									}
									if(list_changed){
										map.put("produced", filtered);
										changed = true;
									}
									
									//need to update them all so we can delete the ones that weren't touched
									changed = true;	//update time was set
									
									
									//batch_update.add(map);
									if(!changed || DbRethinkMap.UpdateIf("plan", id, map, "update_counter", update_counter) > 0){
										success = true;
									}else{
										//map = DbRethinkMap.Get("plan", "mfgnum", mfgnum);
										map = DbRethinkMap.Get("plan", RethinkDB.r.hashMap("mfgnum", mfgnum).with("operation", o.getCode()));
									}
								}
							}
							/*
							if(batch_insert.size() > 100){
								DbRethinkMap.Insert("plan", batch_insert);
								//DbRethinkMap.Update("plan", batch_update);
								batch_insert.clear();
							}
							*/
						}
						if(batch_insert.size() > 0){
							DbRethinkMap.Insert("plan", batch_insert);
							//DbRethinkMap.Update("plan", batch_update);
							batch_insert.clear();
						}
						
						DbRethinkMap.DeleteIfLT("plan", null, "update", now);
						
						
						lock.put("status", 0);
						lock.update("lock", lock.get("id").toString());

	        		}
	        	}
			}catch(Exception e){
				e.printStackTrace();
				SleepWait(60000);
			}
		}
		
		
		mCancellationPending = true;
		
		synchronized (mFeedSync) {
			if(mExecutionGroupsCursor != null)
				mExecutionGroupsCursor.close();
			if(mWorkordersCursor != null)
				mWorkordersCursor.close();
			if(mItemStockCursor != null)
				mItemStockCursor.close();
			if(mPlanCursor != null)
				mPlanCursor.close();
		}
		
		
		mSolver.terminateEarly();
        mBestSolutions.clear();
        
        try {
        	feedGroups.join();
        	feedWorkorders.join();
        	feedItemStocks.join();
        	feedPlans.join();
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
	}
	
	protected List<Resource> loadResources(){
		
		List<Resource> res = new ArrayList<Resource>();
		
		List<DbRethinkMap> wks = DbRethinkMap.GetAll("workstation", 0, 1000);
		
		for(Map<String,Object> m : wks){
			int count = Integer.parseInt(m.get("workstation_count").toString());
			for(int c = 0; c < count; c++){
				Resource r = null;
				r = new Resource();
				r.setCapacity((int)(Float.parseFloat(m.get("workstation_capacity").toString())*3600));
				r.setCode(m.get("workstation_code").toString());
				r.setWorkcenter(m.get("workstation_workcenter").toString());
				r.setInstance(c+1);
				
				//note that Calendar.SUNDAY = 1
				int[] dailyCap = new int[]{ 
						(int)(Float.parseFloat(m.get("workstation_capacity_sun").toString())*3600),
						(int)(Float.parseFloat(m.get("workstation_capacity_mon").toString())*3600),
						(int)(Float.parseFloat(m.get("workstation_capacity_thu").toString())*3600),
						(int)(Float.parseFloat(m.get("workstation_capacity_wed").toString())*3600),
						(int)(Float.parseFloat(m.get("workstation_capacity_tue").toString())*3600),
						(int)(Float.parseFloat(m.get("workstation_capacity_fri").toString())*3600),
						(int)(Float.parseFloat(m.get("workstation_capacity_sat").toString())*3600)
				};
				r.setDailyCapacity(dailyCap);
				
				//r.setOperationList(new ArrayList<Operation>());
				res.add(r);
			}
		}
		return res;
	}
	
	protected int loadExecutionGroups(Schedule sch){
		
		
		int res = 0;
		ScoreDirector dummy = new NullScoreDirector(sch);
		List<DbRethinkMap> groups = DbRethinkMap.GetAll("execution_group", 0, 50000);
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(groups);
		OnExecutionGroupChanged(dummy, list);
		res = list.size();
		/*
		for(Map<String,Object> group : groups){
			OnExecutionGroupChanged(dummy, group);
        	
        	res++;
		}
		*/
		return res;
		
	}
	
	protected void debugFixWorkOrders(){
		
		List<DbRethinkMap> workorders = DbRethinkMap.GetAll("workorder", 0, 100000);
		
		for(Map<String, Object> wo : workorders){
			boolean update = false;
			List<Map<String, Object>> items = (List<Map<String, Object>>) wo.get("items");
			int total = 0;
			int completed = 0;
			for(Map<String, Object> it : items){
				if(!it.containsKey("ITMSTA")){
					it.put("ITMSTA", 1L);
					update = true;
				}
				String pjt = it.get("PJT").toString();
				if(!pjt.startsWith("201627")){
					it.put("ITMSTA",3L);
					update = true;
					completed++;
				}
				total++;
			}
			
			if(completed >= total){
				wo.put("MFGSTA", 4L);
				update = true;
			}
			
			if(update){
				DbRethinkMap map = new DbRethinkMap(wo);
				map.update("workorder", wo.get("MFGNUM").toString());
			}
		}
		
	}
	
	protected int loadWorkOrders(Schedule sch){
		int res = 0;
		ScoreDirector dummy = new NullScoreDirector(sch);
		//List<DbRethinkMap> workorders = DbRethinkMap.GetAll("workorder", 0, 20000);
		HashMap<String,String> filter = new HashMap<>();
		filter.put("status", "MOD");
		List<DbRethinkMap> workorders = DbRethinkMap.GetAll("workorder",0, 20000, filter); 
		
		/*
		DbRethinkMap emptyWo = new DbRethinkMap();
		emptyWo.put("MFGNUM", "dummy");
		emptyWo.put("MFGSTA", 1L);
		emptyWo.put("STRDAT", System.currentTimeMillis());
		emptyWo.put("ENDDAT", System.currentTimeMillis());
		emptyWo.put("status", "MOD");
		
		workorders.add(emptyWo);
		*/
		List<Map<String,Object>> list = null;
		list = new ArrayList<Map<String,Object>>(workorders);
		
		
		OnWorkOrderChanged(dummy, list);
		
		test(dummy);
		
		res = list.size();
		/*
		for(Map<String,Object> wo : workorders){
			OnWorkOrderChanged(dummy, wo);
        	res++;
		}
		*/
		return res;
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
	
	protected void OnWorkOrderChanged(final Map<String,Object> wo) {
		List<Map<String,Object>> l = new ArrayList<Map<String,Object>>(1);
    	l.add(wo);
    	OnWorkOrderChanged(l);
    }
	
	protected void OnWorkOrderChanged(List<Map<String,Object>> wos) {
        mSolver.addProblemFactChange(new ProblemFactChange() {
            public void doChange(ScoreDirector scoreDirector) {
            	synchronized (mWaitProblemFactChange) {
            		mWaitProblemFactChange.notifyAll();
            	}
            	OnWorkOrderChanged(scoreDirector, wos);
            }
        });
    }
	
	protected void OnWorkOrderChanged(ScoreDirector scoreDirector, final List<Map<String,Object>> wos) {
		
		boolean stock_item_list_changed = false;
    	boolean order_created = false;
    	boolean order_modified = false;
		

    	Schedule sch = (Schedule) scoreDirector.getWorkingSolution();
    	// A SolutionCloner does not clone problem fact lists (such as executionGroups)
    	// Shallow clone the executionGroups so only workingSolution is affected, not bestSolution or guiSolution
    	List<WorkOrder> schOrderList = new ArrayList<WorkOrder>( sch.getWorkOrderList());
    	sch.setWorkOrderList( schOrderList );
    	List<ExecutionGroup> schExecutionGroups = new ArrayList<ExecutionGroup>(sch.getExecutionGroupList()) ;
    	sch.setExecutionGroupList( schExecutionGroups );
    	List<StockItem> schItemList = null;
    	synchronized (sch.getStockItemList()) {
        	schItemList = new ArrayList<StockItem>(sch.getStockItemList()) ;
        	sch.setStockItemList( schItemList );
		
	    	List<Operation> schOperationList = new ArrayList<Operation>(sch.getOperationList()) ;
	    	sch.setOperationList( schOperationList );
	    	
			for(Map<String,Object> wo : wos){
				
				
				
				mSolverBackoff = 0;
				//final Map<String,Object> wo
				String MFGNUM = wo.get("MFGNUM").toString();
				String status = wo.get("status").toString();
				
				long MFGSTA = (Long)wo.getOrDefault("MFGSTA", 1L);
		    	//long ENDDAT = ((OffsetDateTime)wo.get("ENDDAT")).getLong(ChronoField.INSTANT_SECONDS);
				long ENDDAT = (long)wo.get("ENDDAT");
				long update = wo.containsKey("update") ? (long)wo.get("update") : 0;
				if(update > mLastChangeTime)
					mLastChangeTime = update;
	
				boolean deletemfg = MFGSTA >= 4 || "FIN".equals(status);
		    	
				if(!deletemfg){
					//take a peek at items to check if all are satisfied
					List<Map<String, Object>> items = (List<Map<String, Object>>) wo.get("items");
					int count = 0;
					int completed = 0;
					for(Map<String, Object> it : items){
						count++;
						long ITMSTA = (long)it.getOrDefault("ITMSTA", 3L);
						if(ITMSTA >= 3){
							completed++;
						}
					}
					if(completed >= count){
						deletemfg = true;
					}
				}
				
				
		    	WorkOrder order = null;
		    	order = schOrderList.stream().filter(o -> MFGNUM.equals(o.getMFGNUM())).findFirst().orElse(null);
		
		    	
		    	if(deletemfg){
			    	//remove the existent workorder because it just got closed
			    	
			    	if(order != null){
			    		//tear it down
			    		List<Operation> orderOperations = order.getOperations();
			    		for(Operation o : orderOperations){
			        		
			        		
			        		
			    			scoreDirector.beforeEntityRemoved(o);
			    			schOperationList.remove(o);
			        		scoreDirector.afterEntityRemoved(o);
				    		
			        		Resource r = o.getResource();
			        		//Set<Operation> resOps = new HashSet<>(r.getOperationList());
			        		//r.setOperationList(resOps);
			        		
			        		//scoreDirector.beforeProblemFactChanged(r);
			        		//resOps.remove(o);
				    		//scoreDirector.afterProblemFactChanged(r);
				    		order_modified = true;
			    		}
			    		
			    		/*
			    		scoreDirector.beforeProblemFactRemoved(order);
			    		schOrderList.remove(order);
			    		scoreDirector.afterProblemFactRemoved(order);
			    		*/
			    		scoreDirector.beforeEntityRemoved(order);
			    		schOrderList.remove(order);
			    		scoreDirector.afterEntityRemoved(order);
			    		order_modified = true;
			    		
			    		//housekeeping,
			    		//execution groups is a pain in the a.. to sync
			        	List<ExecutionGroup> blackList = new ArrayList<>(schExecutionGroups);
			        	Set<ExecutionGroup> inUse = new HashSet<>();
			        	for(WorkOrder o : schOrderList){
			        		inUse.addAll(o.getExecutionGroups());
			        	}
			        	blackList.removeAll(inUse);
			        	
//			        	for(int i = schExecutionGroups.size() - 1 ; i >= 0; i--){
//			        		ExecutionGroup g = schExecutionGroups.get(i);
//			        		
//				        	int counter = 0;
//				        	for(WorkOrder o : schOrderList){
//				        		if(o.getExecutionGroups().contains(g)){
//				        			counter++;
//				        			break;
//				        		}
//				        	}
//				        	if(counter == 0){
//				        		blackList.add(g);
//				        		//scoreDirector.beforeProblemFactRemoved(g);
//				        		//schExecutionGroups.remove(i);
//				        		//scoreDirector.afterProblemFactRemoved(g);
//				        	}else{
//						    	//final ExecutionGroup g = exg;
//						    	//if(g.getUsageCount() != counter){
//						    	//	scoreDirector.beforeProblemFactChanged(g);
//						    	//	g.setUsageCount((int)counter);
//						    	//	scoreDirector.afterProblemFactChanged(g);
//						    	//}
//				        	}
//			        	}
		        		scoreDirector.beforeProblemFactRemoved(schExecutionGroups);
		        		schExecutionGroups.remove(blackList);
		        		scoreDirector.afterProblemFactRemoved(schExecutionGroups);
			        	order_modified = true;
			        	
			        	for(int i = schItemList.size() - 1 ; i >= 0; i--){
			        		StockItem it = schItemList.get(i);
				        	int counter = 0;
				        	for(WorkOrder o : schOrderList){
				        		if(o.isItemReferenced(it)){
				        			counter++;
				        			break;
				        		}
				        	}
				        	if(counter == 0){
				        		scoreDirector.beforeProblemFactRemoved(it);
				        		schItemList.remove(i);
				        		scoreDirector.afterProblemFactRemoved(it);
				        		//TODO: should we set stock_item_list_changed and make it refresh??
				        	}
			        	}
			        	
			        	//remove it from plans
			        	
			        	
			        	
			        	
			    	}
			    	try {
						mPlansToDelete.put(MFGNUM);
						mWorkordersToClose.put(MFGNUM);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			    	
			    	continue;
		    	}
		    	
		    	
		    	
		    	if(order == null){
			    	order = new WorkOrder();
			    	order.setMFGNUM(MFGNUM);
			    	order.setExecutionGroups(new ArrayList<ExecutionGroup>());
			    	order.setProducedTransaction(new ArrayList<StockItemProductionTransaction>());
			    	order.setRequiredTransaction(new ArrayList<StockItemTransaction>());
			    	order.setOperations(new ArrayList<Operation>());
			    	order_created = true;
		    	}
		    	
		    	List<ExecutionGroup> executionGroups = order.getExecutionGroups();
		    	List<StockItemProductionTransaction> producedItems = order.getProducedTransactionList();
		    	List<StockItemTransaction> consumedItems = order.getRequiredTransaction();
		    	List<Operation> orderOperations = order.getOperations();
		        
		    	
		    	List<String> wo_groups = (List<String>) wo.get("groups");
		    	
		    	//if(!(wo_groups.contains("INV_0011607SON00002784") || wo_groups.contains("INV_0011607SON00002869")))
		    	//	continue;
		    	
		    	
		    	if(wo_groups != null){
		    		for(String g_code : wo_groups){
		            	ExecutionGroup exg = null;
		            	exg = schExecutionGroups.stream().filter(x -> g_code.equals(x.getCode())).findFirst().orElse(null);
		            	/*
		            	for(ExecutionGroup g : schExecutionGroups){
		            		if(g_code.equals(g.getCode())){
		            			exg = g;
		            			break;
		            		}
		            	}
		            	*/
		            	if(exg == null){
		            		exg = new ExecutionGroup(g_code);
		            		exg.setCode(g_code);
		            		//exg.setUsageCount(1);
		            		scoreDirector.beforeProblemFactAdded(exg);
		            		schExecutionGroups.add(exg);
		            		scoreDirector.afterProblemFactAdded(exg);
		            		order_modified = true;
		            	}else{
				    		//scoreDirector.beforeProblemFactChanged(exg);
				    		//exg.setUsageCount(exg.getUsageCount()+1);
				    		//scoreDirector.afterProblemFactChanged(exg);
		            	}
		            		
		            	
		            	if(!executionGroups.contains(exg)){
		            		executionGroups.add(exg);
		            	}
		    		}
		    	}
		    	
		    	//get those items            	
		    	order.setExecutionGroups(executionGroups);//force recalculation of start/end times
		    	
		    	
		    	
		    	List<Map<String, Object>> items = (List<Map<String, Object>>) wo.get("items");
		    	for(Map<String, Object> it : items){
		    		String VCRNUMORI = it.get("VCRNUMORI").toString();
		    		String ITMREF = it.get("ITMREF").toString();
		    		Float UOMEXTQTY = Float.parseFloat(it.get("UOMEXTQTY").toString());
		    		String STU = it.get("STU").toString();
		    		
		        	StockItem exs = null;
		        	exs = schItemList.stream().filter(s -> ITMREF.equals(s.getReference())).findFirst().orElse(null);
		        	
		        	if(exs == null){
		        		exs = new StockItem();
		        		exs.setInitialAmount(0);
		        		exs.setReference(ITMREF);
		        		scoreDirector.beforeProblemFactAdded(exs);
		        		schItemList.add(exs);
		        		stock_item_list_changed = true;
		        		scoreDirector.afterProblemFactAdded(exs);
		        		order_modified = true;
		        	}
		        	final StockItem aux = exs;
		        	StockItemProductionTransaction trans = producedItems.stream().filter(i -> aux.equals(i.getItem())).findFirst().orElse(null);
		        	if(trans == null){
			        	trans = new StockItemProductionTransaction();
			        	trans.setItem(exs);
			        	trans.setQuantity(UOMEXTQTY);
			        	trans.setVCRNUMORI(VCRNUMORI);
			        	trans.setSTU(STU);
			        	producedItems.add(trans);
			        	order_modified = true;
		        	}else{
		        		if(Math.abs(UOMEXTQTY - trans.getQuantity()) > 0.01){
		        			trans.setQuantity(UOMEXTQTY);
		        			order_modified = true;
		        		}
		        	}
		    	}
		    	
		    	//get me components            	
		    	List<Map<String, Object>> components = (List<Map<String, Object>>) wo.get("components");
		    	for(Map<String, Object> cmp : components){
		    		String ITMREF = cmp.get("ITMREF").toString();
		    		Float AVAQTY = Float.parseFloat(cmp.get("AVAQTY").toString());	//available
		    		Float RETQTY = Float.parseFloat(cmp.get("RETQTY").toString());	//required
		    		String STU = cmp.get("STU").toString();
		    		
		    		
		        	StockItem exs = null;
		        	exs = schItemList.stream().filter(s -> ITMREF.equals(s.getReference())).findFirst().orElse(null);
		        	
		        	if(exs == null){
		        		exs = new StockItem();
		        		exs.setInitialAmount(AVAQTY);
		        		exs.setReference(ITMREF);
		        		scoreDirector.beforeProblemFactAdded(exs);
		        		schItemList.add(exs);
		        		scoreDirector.afterProblemFactAdded(exs);
		        		stock_item_list_changed = true;
		        		order_modified = true;
		        	}
		        	
		        	
		        	
		        	final StockItem aux = exs;
		        	StockItemTransaction trans = consumedItems.stream().filter(i -> aux.equals(i.getItem())).findFirst().orElse(null);
		        	if(trans == null){
		        		trans = new StockItemTransaction();
		            	trans.setItem(exs);
		            	trans.setQuantity(RETQTY);
		            	trans.setSTU(STU);
		            	consumedItems.add(trans);
			        	order_modified = true;
		        	}else{
		        		if(Math.abs(RETQTY - trans.getQuantity()) > 0.01){
		        			trans.setQuantity(RETQTY);
		        			order_modified = true;
		        		}
		        	}
		    	}
		    	
		    	
		    	//on to the operations
		    	
		    	
		    	List<Map<String, Object>> operations = (List<Map<String, Object>>) wo.get("operations");
		    	Long firstStart = null;
		    	Long lastEnd = null;
		    	for(Map<String, Object> o : operations){
		    		String id = o.containsKey("id") ? o.get("id").toString() : order.getMFGNUM();
		    		String EXTWST = o.get("EXTWST").toString();
		    		String WCR = o.get("WCR").toString();
		    		String OPEUOM = o.get("OPEUOM").toString();
		    		String TIMUOMCOD = o.containsKey("TIMUOMCOD") ? o.get("TIMUOMCOD").toString() : "";
		    		int OPENUM = Integer.parseInt(o.get("OPENUM").toString());
		    		float tim = Float.parseFloat( o.get("EXTOPETIM").toString() );
		    		
		    		if("1".equalsIgnoreCase(TIMUOMCOD)){
		    			getLogger().warn("TIMUOMCOD indicates HOURS ... this is likely an error so replacing by minutes in "+MFGNUM);
		    			TIMUOMCOD = "2";
		    		}
		    		
		    		int EXTOPETIM = (int) ("1".equalsIgnoreCase(TIMUOMCOD) ? tim*3600 : tim*60 );
		    		if(EXTOPETIM == 0){
		    			getLogger().warn("EXTOPETIM is 0 ... this is likely an error so replacing by 1s in "+MFGNUM);
		    			EXTOPETIM = 1;
		    		}
		    		
		    		//EXTOPETIM = 1; //FIXME
		    		
		    		//Long FRCSTR = o.get("FRCSTR") != null ? ((OffsetDateTime)o.get("FRCSTR")).getLong(ChronoField.INSTANT_SECONDS) : System.currentTimeMillis()/1000;
		    		//Long start = o.get("start") != null ? ((OffsetDateTime)o.get("start")).getLong(ChronoField.INSTANT_SECONDS) : (System.currentTimeMillis()/1000) + 2*PLANNING_WINDOW_OFFSET;
		    		long start = ((Long)o.getOrDefault("OPESTR", System.currentTimeMillis()))/1000;
		    		
		    		
		    		
		    		if(orderOperations.stream().anyMatch(op -> id.equals(op.getCode()))){	//the untouched code allways exists so we can check for the original code
		    			continue;
		    		}
		    		
		    		
		    		//List<Resource> oResourceList = new ArrayList<Resource>();
		    		List<Resource> oResourceList = sch.getResourceList().stream().filter(r -> EXTWST.equals(r.getCode()) && WCR.equals(r.getWorkcenter())).collect(Collectors.toList());
		    		//oResourceList = new ArrayList<>(oResourceList);
		    		Resource res = oResourceList.get(0);
		    		//Set<Operation> resourceOps = new HashSet<Operation>(res.getOperationList());
		    		//res.setOperationList(resourceOps);
		    		int counter = 0;
//		    		boolean isMovable = true;
//		    		if("EXPE".equals(res.getCode())){
//		    			isMovable = false;
//		    			start 
//		    		}
		    		do{
		    		
		    			int scheduledTime = Math.min(EXTOPETIM, 4*3600);	//limit to 4 hours batch maximum 
		    			
			        	Operation op = new Operation();
			        	op.setCode(counter == 0 ? id : id + "."+counter);
			        	op.setOPENUM((int)OPENUM);
			        	op.setDuration(scheduledTime);
			        	op.setResource(res);
			        	op.setResourceRange(oResourceList);
			        	op.setResourceRequirement(scheduledTime);
			        	op.setStartDate(start);
			        	
			        	op.setWorkOrder(order);
			        	
		        		//scoreDirector.beforeProblemFactChanged(res);
		        		//resourceOps.add(op);
			    		//scoreDirector.afterProblemFactChanged(res);

			        	
			        	
			        	Long s = op.getStartDate();
			    		Long e = op.getEndDate();
			    		if(firstStart == null || s < firstStart)
			    			firstStart = s;
			    		if(lastEnd == null || e > lastEnd)
			    			lastEnd = e;
			    		
			    		
			    		//scoreDirector.beforeVariableChanged(order, "firstStart");
			    		//order.setFirstStart(firstStart == null ? System.currentTimeMillis()/1000 : firstStart);
				    	//order.setLastEnd( lastEnd == null ? System.currentTimeMillis()/1000 : lastEnd);
				    	//scoreDirector.afterVariableChanged(order, "firstStart");
				    	
			        	orderOperations.add(op);
			        	
			        	scoreDirector.beforeEntityAdded(op);
			        	assert(!schOperationList.contains(op));
			        	schOperationList.add(op);
			    		scoreDirector.afterEntityAdded(op);
			    		order_modified = true;
			    		
			    		EXTOPETIM -= scheduledTime;
			    		counter++;
		    		}while(EXTOPETIM > 0);
		    		
		    	}
		    	
		    	if(order_created){
		    		//scoreDirector.beforeVariableChanged(order, "firstStart");
			    	//order.setFirstStart(firstStart == null ? System.currentTimeMillis()/1000 : firstStart);
			    	//order.setLastEnd( lastEnd == null ? System.currentTimeMillis()/1000 : lastEnd);
			    	//scoreDirector.afterVariableChanged(order, "firstStart");
			    	//order_modified = true;
			    	
			    	scoreDirector.beforeEntityAdded(order);
			    	schOrderList.add(order);
			    	order.setFirstStart(firstStart == null ? System.currentTimeMillis()/1000 : firstStart);
			    	order.setLastEnd( lastEnd == null ? System.currentTimeMillis()/1000 : lastEnd);
					scoreDirector.afterEntityAdded(order);
			    	/*
			    	scoreDirector.beforeProblemFactAdded(order);
			    	schOrderList.add(order);
		    		scoreDirector.afterProblemFactAdded(order);
			    	*/
			    	
		    	}else if(order_modified){
		    		
		    		scoreDirector.beforeVariableChanged(order, "firstStart");	//just make it recalculate
			    	order.setFirstStart(firstStart == null ? System.currentTimeMillis()/1000 : firstStart);
			    	order.setLastEnd( lastEnd == null ? System.currentTimeMillis()/1000 : lastEnd);
		    		scoreDirector.afterVariableChanged(order, "firstStart");
		    		
		    	}
			}
    	}
    	if(order_created || order_modified)
    		scoreDirector.triggerVariableListeners();
    	
    	
    	if(stock_item_list_changed){
    		
    		synchronized (mFeedSync) {
    			if(mItemStockCursor != null)
    				mItemStockCursor.close();	//cause reload on the feed
			}
    		
    	}
    	
	}
	
	
	
	protected void OnExecutionGroupChanged(final Map<String,Object> group) {
    	List<Map<String,Object>> l = new ArrayList<Map<String,Object>>(1);
    	l.add(group);
    	OnExecutionGroupChanged(l);
    }
	protected void OnExecutionGroupChanged(final List<Map<String,Object>> groups) {
        mSolver.addProblemFactChange(new ProblemFactChange() {
            public void doChange(ScoreDirector scoreDirector) {
            	synchronized (mWaitProblemFactChange) {
            		mWaitProblemFactChange.notifyAll();
            	}

            	OnExecutionGroupChanged(scoreDirector, groups);
            }
        });
    }
	
	protected void OnExecutionGroupChanged(ScoreDirector scoreDirector, final List<Map<String,Object>> groups) {
		
    	Schedule sch = (Schedule) scoreDirector.getWorkingSolution();
    	// A SolutionCloner does not clone problem fact lists (such as executionGroups)
    	// Shallow clone the executionGroups so only workingSolution is affected, not bestSolution or guiSolution
    	List<ExecutionGroup> schExecutionGroups = new ArrayList<ExecutionGroup>(sch.getExecutionGroupList()) ;
    	sch.setExecutionGroupList( schExecutionGroups );
		
    	boolean changes = false;
    	
		for(Map<String,Object> group : groups){
		
			boolean exg_changes = false;
			
			String code = group.get("code").toString();
	    	//String description = group.get("description").toString();
	    	//OffsetDateTime start = (OffsetDateTime)group.get("start");
	    	//OffsetDateTime end = (OffsetDateTime)group.get("end");
			Long start = (Long)group.get("start");
			Long end = (Long)group.get("end");
	    	boolean is_manual = (boolean)group.get("is_manual");
	    	
	    	start = start == null ? -1 : start/1000;
	    	end = end == null ? -1 : end/1000;
	    	
	    	ExecutionGroup exg = null;
	    	for(ExecutionGroup g : schExecutionGroups){
	    		if(code.equals(g.getCode())){
	    			exg = g;
	    			break;
	    		}
	    	}
	    	if(exg == null){
	    		exg = new ExecutionGroup(code);
	    		exg.setCode(code);
		    	exg.setStartTime( start > 0 ? start : null );
		    	exg.setEndTime(end > 0 ? end : null);
		    	exg.setManual(is_manual);
	    		
	    		scoreDirector.beforeProblemFactAdded(exg);
	    		schExecutionGroups.add(exg);
	    		scoreDirector.afterProblemFactAdded(exg);
	    		exg_changes = true;
	    	}else{
	    		if(1*start != 1*(exg.getStartTime() == null ? -1 : exg.getStartTime()))
	    			exg_changes = true;
	    		else if(1*end != 1*(exg.getEndTime() == null ? -1 : exg.getEndTime()))
	    			exg_changes = true;
	    		else if(is_manual != exg.isManual())
	    			exg_changes = true;
	    		if(exg_changes){
			    	
	
		    		scoreDirector.beforeProblemFactChanged(exg);
		    		exg.setStartTime( start > 0 ? start : null );
			    	exg.setEndTime(end > 0 ? end : null);
			    	exg.setManual(is_manual);
		    		//schExecutionGroups.add(exg);
		    		scoreDirector.afterProblemFactChanged(exg);
	    		}
		    	
	    	}
	    	if(exg_changes)
	    		changes = true;
	    	
	    	//List<WorkOrder> wos = sch.getWorkOrderList();
	    	//final ExecutionGroup g = exg;
	    	//long c = wos.stream().filter(w -> w.getExecutionGroups().contains(g)).count();
	    	//if(exg.getUsageCount() != c){
	    	//	scoreDirector.beforeProblemFactChanged(exg);
	    	//	exg.setUsageCount((int)c);
	    	//	scoreDirector.afterProblemFactChanged(exg);
	    	//}
	    	
		}
		
		if(changes)
			scoreDirector.triggerVariableListeners();
	}
	
	
	protected int loadStockItems(Schedule sch){
		int res = 0;
		ScoreDirector dummy = new NullScoreDirector(sch);

		List<StockItem> stockItemList = sch.getStockItemList();
		
		if(stockItemList == null || stockItemList.size() == 0){
			return 0;
		}
		
		String[] currentItems = null;
		currentItems = new String[stockItemList.size()];
		for(int i = 0; i < stockItemList.size(); i++){
			currentItems[i] = stockItemList.get(i).getReference();
		}
		
		List<DbRethinkMap> items = DbRethinkMap.Get("itmmaster", "itmref", currentItems);	
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(items);
		
		OnStockItemChanged(dummy, list);
		res = list.size();
		return res;
	}
	
	
	protected void OnStockItemChanged(final Map<String,Object> item) {
		List<Map<String,Object>> items = new ArrayList<Map<String,Object>>();
		items.add(item);
		OnStockItemChanged(items);
    }
	protected void OnStockItemChanged(final List<Map<String,Object>> items) {
        mSolver.addProblemFactChange(new ProblemFactChange() {
            public void doChange(ScoreDirector scoreDirector) {
            	synchronized (mWaitProblemFactChange) {
            		mWaitProblemFactChange.notifyAll();
            	}

            	OnStockItemChanged(scoreDirector, items);
            }
        });
    }
	protected void OnStockItemChanged(ScoreDirector scoreDirector, final List<Map<String,Object>> items) {
		boolean changes = false;
    	Schedule sch = (Schedule) scoreDirector.getWorkingSolution();    	
    	// A SolutionCloner does not clone problem fact lists (such as executionGroups)
    	// Shallow clone the executionGroups so only workingSolution is affected, not bestSolution or guiSolution
    	synchronized (sch.getStockItemList()) {
	    	List<StockItem> schStockItems = new ArrayList<StockItem>(sch.getStockItemList()) ;
	    	sch.setStockItemList( schStockItems );
			for(Map<String,Object> item : items){
		
				String itmref = item.get("itmref").toString();
				String tclcode = item.containsKey("tclcode") ? item.get("tclcode").toString() : "";
				Float physto = Float.parseFloat(item.get("physto").toString());
				long update = item.containsKey("updstp") ? (long)item.get("updstp") : 0;
				if(update > mLastChangeTime)
					mLastChangeTime = update;

				long reocod = item.containsKey("reocod") ? (long)item.get("reocod") : 3;	//3 - manufacture
	    	
		    	StockItem exs = null;
		    	exs = schStockItems.stream().filter(s -> itmref.equals(s.getReference())).findFirst().orElse(null);
		    	
		    	if(exs != null){
		    		if((Math.abs(exs.getInitialAmount() - physto) > 0.001) || (reocod != exs.getReocod())){
			    		scoreDirector.beforeProblemFactChanged(exs);
			    		exs.setInitialAmount(physto);
			    		exs.setReocod((int)reocod);
			    		exs.setCategory(tclcode);
			    		scoreDirector.afterProblemFactChanged(exs);
			    		changes = true;
		    		}
		    	}
	    	}
		}
		
		if(changes)
    		scoreDirector.triggerVariableListeners();

	}
	
	protected int loadCurrentPlans(Schedule sch){
		int res = 0;
		ScoreDirector dummy = new NullScoreDirector(sch);
		List<DbRethinkMap> plans = DbRethinkMap.GetAll("plan", 0, -1);
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(plans);
		
		OnPlanChanged(dummy, list);
		res = list.size();
		return res;
	}
	
	protected void OnPlanChanged(final Map<String,Object> item) {
		final List<Map<String,Object>> items = new ArrayList<Map<String,Object>>(1);
		items.add(item);
		OnPlanChanged(items);
    }

	private void OnPlanChanged(List<Map<String,Object>> items) {
		mSolver.addProblemFactChange(new ProblemFactChange() {
            public void doChange(ScoreDirector scoreDirector) {
            	synchronized (mWaitProblemFactChange) {
            		mWaitProblemFactChange.notifyAll();
            	}

            	OnPlanChanged(scoreDirector, items);
            }
        });
	}
	protected void OnPlanChanged(ScoreDirector scoreDirector, final List<Map<String,Object>> items) {
		
		boolean mods = false;
    	Schedule sch = (Schedule) scoreDirector.getWorkingSolution();    	
    	// A SolutionCloner does not clone problem fact lists (such as executionGroups)
    	// Shallow clone the executionGroups so only workingSolution is affected, not bestSolution or guiSolution
    	List<Operation> schOperations = new ArrayList<Operation>(sch.getOperationList()) ;
    	sch.setOperationList( schOperations );
		
		for(Map<String,Object> item : items){
		
			boolean assigned = (boolean)item.get("assigned");
			boolean completed = (boolean)item.get("completed");
			
			
			long start = ((Long) item.get("start"))/1000;
			//long end = ((OffsetDateTime) item.get("end")).toEpochSecond();
			//long duration = (Long) item.get("duration");
			String workcenter = (String) item.get("workcenter");
			String workstation = (String) item.get("workstation");
			long workstation_number = (Long) item.get("workstation_number");
			String plannerId = item.getOrDefault("planner_id", "").toString();
			
			String mfgnum = item.get("mfgnum").toString();
			String operation = item.get("operation").toString();
			
	    	Operation ex = null;
	    	ex = schOperations.stream().filter(o -> (mfgnum.equals(o.getWorkOrder().getMFGNUM()) && operation.equals(o.getCode()))).findFirst().orElse(null);
	    	
	    	if(ex == null){
	    		getLogger().error("Invalid plan change received...");
	    		continue;
	    	}
	    	
	    	
	    	boolean moveable = !(assigned || completed);
			//if(assigned || !mPlannerUUID.equals(plannerId))
	    	ex.setMovable(moveable);

	    	
	    	if(ex.getStartDate() != start){
	    		scoreDirector.beforeVariableChanged(ex, "startDate");
	    		//scoreDirector.beforeVariableChanged(ex.getWorkOrder(), "firstStart");	//just make it recalculate
	    		ex.setStartDate(start);
	    		scoreDirector.afterVariableChanged(ex, "startDate");
	    		
	    		(new OperationDateUpdatingVariableListener()).afterVariableChanged(scoreDirector, ex);
	    		//scoreDirector.afterVariableChanged(ex.getWorkOrder(), "firstStart");
	    		
		    	//order.setFirstStart(firstStart == null ? System.currentTimeMillis()/1000 : firstStart);
		    	//order.setLastEnd( lastEnd == null ? System.currentTimeMillis()/1000 : lastEnd);
	    		mods = true;
    		}
    		
    		
			Resource res = sch.getResourceList().stream().filter(r -> (workcenter.equals(r.getWorkcenter()) && workstation.equals(r.getCode()) && (workstation_number == r.getInstance()))).findFirst().orElse(null);
			if(res != null){
				if(!res.equals(ex.getResource())){
					assert(ex.getResourceRange().contains(res));
					//ex.getResource().getOperationList().remove(ex);
					scoreDirector.beforeVariableChanged(ex, "resource");
		    		ex.setResource(res);
		    		scoreDirector.afterVariableChanged(ex, "resource");
		    		//res.getOperationList().add(ex);
		    		mods = true;
				}
			}else{
				getLogger().error("Unable to find resource...");
			}
	    	
		}
    	if(mods){
    		scoreDirector.triggerVariableListeners();
    	}
    	
	}
	
	/**
	 * Returns an <code>InetAddress</code> object encapsulating what is most likely the machine's LAN IP address.
	 * <p/>
	 * This method is intended for use as a replacement of JDK method <code>InetAddress.getLocalHost</code>, because
	 * that method is ambiguous on Linux systems. Linux systems enumerate the loopback network interface the same
	 * way as regular LAN network interfaces, but the JDK <code>InetAddress.getLocalHost</code> method does not
	 * specify the algorithm used to select the address returned under such circumstances, and will often return the
	 * loopback address, which is not valid for network communication. Details
	 * <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4665037">here</a>.
	 * <p/>
	 * This method will scan all IP addresses on all network interfaces on the host machine to determine the IP address
	 * most likely to be the machine's LAN address. If the machine has multiple IP addresses, this method will prefer
	 * a site-local IP address (e.g. 192.168.x.x or 10.10.x.x, usually IPv4) if the machine has one (and will return the
	 * first site-local address if the machine has more than one), but if the machine does not hold a site-local
	 * address, this method will return simply the first non-loopback address found (IPv4 or IPv6).
	 * <p/>
	 * If this method cannot find a non-loopback address using this selection algorithm, it will fall back to
	 * calling and returning the result of JDK method <code>InetAddress.getLocalHost</code>.
	 * <p/>
	 *
	 * @throws UnknownHostException If the LAN address of the machine cannot be found.
	 */
	private static InetAddress getLocalHostLANAddress() throws UnknownHostException {
	    try {
	        InetAddress candidateAddress = null;
	        // Iterate all NICs (network interface cards)...
	        for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
	            NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
	            // Iterate all IP addresses assigned to each card...
	            for (Enumeration<InetAddress> inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
	                InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
	                if (!inetAddr.isLoopbackAddress()) {

	                    if (inetAddr.isSiteLocalAddress()) {
	                        // Found non-loopback site-local address. Return it immediately...
	                        return inetAddr;
	                    }
	                    else if (candidateAddress == null) {
	                        // Found non-loopback address, but not necessarily site-local.
	                        // Store it as a candidate to be returned if site-local address is not subsequently found...
	                        candidateAddress = inetAddr;
	                        // Note that we don't repeatedly assign non-loopback non-site-local addresses as candidates,
	                        // only the first. For subsequent iterations, candidate will be non-null.
	                    }
	                }
	            }
	        }
	        if (candidateAddress != null) {
	            // We did not find a site-local address, but we found some other non-loopback address.
	            // Server might have a non-site-local address assigned to its NIC (or it might be running
	            // IPv6 which deprecates the "site-local" concept).
	            // Return this non-loopback candidate address...
	            return candidateAddress;
	        }
	        // At this point, we did not find a non-loopback address.
	        // Fall back to returning whatever InetAddress.getLocalHost() returns...
	        InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
	        if (jdkSuppliedAddress == null) {
	            throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
	        }
	        return jdkSuppliedAddress;
	    }
	    catch (Exception e) {
	        UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
	        unknownHostException.initCause(e);
	        throw unknownHostException;
	    }
	}
	
	public static void main(String[] args) throws InterruptedException{
		
		
		DbRethinkMap.setParameters("sig.sotubo.pt", 28015, "planner",  "");
		//DbRethinkMap.setParameters("192.168.10.240", 28015, "planner",  "");
		
		
		Planner p = new Planner();
		
		Thread t = new Thread(p);
		
		
		t.start();
		
		t.join();
		
	}
	
}
