package com.joker.planner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoField;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joker.planner.domain.ExecutionGroup;
import com.joker.planner.domain.Operation;
import com.joker.planner.domain.Resource;
import com.joker.planner.domain.Schedule;
import com.joker.planner.domain.StockItem;
import com.joker.planner.domain.StockItemProductionTransaction;
import com.joker.planner.domain.StockItemTransaction;
import com.joker.planner.domain.WorkOrder;
import com.joker.planner.solver.BaseSolver;
import com.joker.planner.solver.BruteForceSolver;
import com.joker.planner.solver.NoSolver;
import com.joker.planner.solver.RLSolver;
import com.joker.planner.solver.Score;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;


public class Planner<T extends BaseSolver> implements Runnable{

	//public static final String SOLVER_CONFIG = "pt/sotubo/planner/projectJobSchedulingSolverConfig.xml";
	public static final String SOLVER_CONFIG = "projectJobSchedulingSolverConfig.xml";
	 /**
     * offset from current time for planning window
     */
	public static final int PLANNING_WINDOW_OFFSET = 4*3600;
    protected transient Logger logger = null;
    private Object mWaitHandle;
    private Object mWaitProblemFactChange;
    private T mSolver;
    private int mExecutionGroupCounter;
    private int mWorkorderCounter;
    private long mWorkorderLoadedUpdateMark; // max update field while batch loading WOs
    
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
    private ArrayBlockingQueue<String> mPlansToDelete;
    private ArrayBlockingQueue<String> mItemsToReload;
    
    public Planner(){
    	//System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    	logger =  LoggerFactory.getLogger(Planner.class);
        mSolver = null;
        mWaitHandle = new Object();
        mWaitProblemFactChange = new Object();
        mCancellationPending = false;
        mPlannerUUID = UUID.randomUUID().toString();// DbRethinkMap.UUID();
        
        mWorkorderLoadedUpdateMark = 0;
        
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
        mPlansToDelete = new ArrayBlockingQueue<String>(20000);
        mItemsToReload = new ArrayBlockingQueue<String>(20000);
        
    }
    
    
    protected void Cancel() {
    	mCancellationPending = true;
    	synchronized (mWaitProblemFactChange) {
    		mWaitProblemFactChange.notifyAll();
    	}
    	synchronized (mWaitHandle) {
    		mWaitHandle.notifyAll();
    	}
    	try {
			mPlansToDelete.put("");
			mItemsToReload.put("");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
							//DbRethinkMap.Update("plan", id, RethinkDB.r.hashMap("deleted", true));
							List<DbRethinkMap> plans = DbRethinkMap.GetAll("plan", 0, 1000, RethinkDB.r.hashMap("mfgnum", mfgnum));
							for(DbRethinkMap m : plans){
								m.put("deleted", true);
								long t = System.currentTimeMillis();
								m.put("update", t);
								m.put("update_planner", t);
							}
							DbRethinkMap.Update("plan", plans);
							
							//DbRethinkMap.Delete("plan", RethinkDB.r.hashMap("mfgnum", mfgnum));
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
		
		Thread reloadItems = new Thread(new Runnable() {
			
			@Override
			public void run() {
				String itmref = "";
				try {
					while((itmref = mItemsToReload.take()) != ""){
						try{
							Map<String, Object> item = DbRethinkMap.Get("itmmaster", "itmref", itmref);
							if(item != null){
								List<Map<String, Object>> l = new ArrayList<>(1);
								l.add(item);
								OnStockItemChanged(l);
							}
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
		reloadItems.setName("reloadItems");
		reloadItems.start();
		
		
    }
    
    
    
    private Schedule mSchedule;
	@Override
	public void run() {
		
		
		
		//debugFixWorkOrders();
		//int res = DbRethinkMap.DeleteIfLT("plan", null, "update", 0);
		
		//assume resource won't change
		mSchedule = new Schedule();
		mSolver = (T) newSolver(mSchedule);
		getLogger().info("Loading resources...");
		mSchedule.setResourceList(loadResources());
		getLogger().info("Loading execution groups...");
		//mExecutionGroupCounter = loadExecutionGroups(mSchedule);
		getLogger().info("Loading work orders...");
		mWorkorderCounter = loadWorkOrders();
		getLogger().info("Loading stock...");
		loadStockItems(); 
		mItemsToReload.clear();
		getLogger().info("Loading previous plans...");
		loadCurrentPlans();
		
		
		
		
		try {
			SerializationUtils.serialize(mSchedule, new FileOutputStream("schedule.data"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		
		/*
		try {
			mSchedule = (Schedule) SerializationUtils.deserialize(new FileInputStream("schedule.data"));
			//mSchedule = (Schedule) SerializationUtils.deserialize(new FileInputStream("solution_opta.data"));
			mSolver = new BruteForceSolver(mSchedule);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		

		/*
		//NullScoreDirector mock = new NullScoreDirector(mSchedule);
		//OperationDateUpdatingVariableListener upd = new OperationDateUpdatingVariableListener();
		int tim = 0;
		for(Operation o : mSchedule.getOperationList()){
			if("EXPE".equals(o.getResource().getCode()))
				continue;
			o.setStartDate(o.getStartDate() + tim);
			tim++;
			//upd.afterVariableChanged(mock, o);
		}
		
		try {
			SerializationUtils.serialize(mSchedule, new FileOutputStream("schedule.data"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		
		
		updateThreads();
		
		BlockingQueue<Schedule> mBestSolutions = new ArrayBlockingQueue<Schedule>(100);
		getLogger().info("Listening for updates...");
		
		mSolver.addNewBestSolutionListener(new BaseSolver.NewBestSolutionListener() {
			
			@Override
			public void OnNewBestSolution(BaseSolver solver, Score score) {
				
				Schedule cl = solver.getBestScheduleClone();
				
				if(!mBestSolutions.isEmpty())
					mBestSolutions.clear();
				try {
					mBestSolutions.put(cl);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			}
		});
		
        Thread feedWorkorders = new Thread(new Runnable() {
			
        	@Override
			public void run() {
				while(!isCancellationPending()){
					
					try{
						
						int state = 0;
						
						try{
							synchronized (mFeedSync) {
								//mWorkordersCursor = DbRethinkMap.GetAll("workorder", mWorkorderCounter, RethinkDB.r.hashMap("status", "MOD"));
								mWorkordersCursor = DbRethinkMap.GetOpenMfg(mWorkorderLoadedUpdateMark);
							}
						
						
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
												SleepWait(10000);
												if(mWorkordersCursor.bufferedSize() > 0)
													state = 0;
												else
													state = 2;
											}
											break;
									case	2:
											List<Map<String,Object>> deletes = new ArrayList<Map<String,Object>>();
											List<Map<String,Object>> mods = new ArrayList<Map<String,Object>>();
											for(Map<String,Object> m : buffer){
												if(m.get("new_val") == null){
													Map<String, Object> wo = (Map<String, Object>) m.get("old_val");
													wo.put("deleted", true);
													deletes.add(wo);
												}else{
													mods.add((Map<String, Object>) m.get("new_val"));
												}
											}
											buffer.clear();
											OnWorkOrderChanged(mods);
											OnWorkOrderChanged(deletes);
											
											mSolverBackoff = 0;
											mWorkorderCounter += deletes.size() + mods.size();
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
							if(mWorkordersCursor != null){
								try{ mWorkordersCursor.close(); } catch (Exception cl) { cl.printStackTrace(); }
							}
							mWorkordersCursor = null;
							SleepWait(5000);						
						}finally{
							if(mWorkordersCursor != null){
								try{ mWorkordersCursor.close(); } catch (Exception cl) { cl.printStackTrace(); }
							}
							mWorkordersCursor = null;
							state = 0;
						}
					}catch(Exception e){
						
						getLogger().error(e.getMessage());
						SleepWait(5*60000);
					}
				}
			}
		});
        feedWorkorders.setName("FeedWorkorders");
        feedWorkorders.start();
        
        
        Thread feedItemStocks = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(!isCancellationPending()){
					int state = 0;
					
					
					
					try{
						synchronized (mFeedSync) {
							mItemStockCursor = DbRethinkMap.GetAll("itmmaster", 0);	
						}
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
						if(mItemStockCursor != null){
							try{ mItemStockCursor.close(); } catch (Exception cl) { cl.printStackTrace(); }
						}
						mItemStockCursor = null;
						SleepWait(5000);						
					}finally{
						if(mItemStockCursor != null){
							try{ mItemStockCursor.close(); } catch (Exception cl) { cl.printStackTrace(); }
						}
						mItemStockCursor = null;
						state = 0;
					}			
				}
			}
			
		});
        feedItemStocks.setName("FeedItemStocks");
        feedItemStocks.start();
        

        Thread feedPlans = new Thread(new Runnable() {
			
        	
			public void run_() {
        		
        		mPlanCursor = null;
        		ArrayList<Map<String,Object>> buffer = new ArrayList<Map<String,Object>>();
        		
        		
        		
        		while(!isCancellationPending()){
        			try{
						synchronized (mFeedSync) {
							mPlanCursor = DbRethinkMap.GetAll("plan", 0);
						}
				
						while(!isCancellationPending()){
							buffer.clear();
							Map<String, Object> pl = null;
							try{
								while( buffer.size() < 100 && (pl = mPlanCursor.next(10000)) != null){
									//boolean assigned = (boolean) pl.getOrDefault("assigned", false);
									//boolean completed = (boolean) pl.getOrDefault("completed", false);
									String plannerId = pl.getOrDefault("planner_id", "").toString();
									long update = ((Number)pl.get("update")).longValue();
									long update_planner = ((Number)pl.getOrDefault("update_planner", update)).longValue();
									//String mfgnum = pl.get("mfgnum").toString();
									//String operation = pl.get("operation").toString();
									
									if(mPlannerUUID.equals(plannerId) && update == update_planner)
										continue;	//updated by me last time
									buffer.add(pl);
								}
							}catch(TimeoutException t){
								
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
						Cursor<HashMap> c = mPlanCursor;
						if(c != null){
							try{
								c.close();
							}catch(Exception e){
								e.printStackTrace();								
							}
						}
						mPlanCursor = null;
					}
        		}
			}
        	
        	
        	@Override
			public void run() {
				while(!isCancellationPending()){
					int state = 0;
					
					
					
					try{
						synchronized (mFeedSync) {
							mPlanCursor = DbRethinkMap.GetAll("plan", 0);	
						}
						
						List<Map<String,Object>> buffer = new ArrayList<Map<String,Object>>(100);
						while(!isCancellationPending() && mPlanCursor != null){
							switch(state){
								case	0:
										Map<String,Object> pl = mPlanCursor.next();
										if(pl != null){
											
											String plannerId = pl.getOrDefault("planner_id", "").toString();
											long update = ((Number)pl.get("update")).longValue();
											long update_planner = ((Number)pl.getOrDefault("update_planner", update)).longValue();
											//String mfgnum = pl.get("mfgnum").toString();
											//String operation = pl.get("operation").toString();
											
											if(!(mPlannerUUID.equals(plannerId) && update == update_planner)){ //ignore updated by me last time
												buffer.add(pl);
												mSolverBackoff = 0;
												state = 1;
											}
										}else{
											mPlanCursor.close();
											mPlanCursor = null;
										}
										break;
								case	1:
										if(buffer.size() >= 100){
											state = 2;
										}else if(mPlanCursor.bufferedSize() > 0){
											state = 0;
										}else{
											SleepWait(1000);
											if(mPlanCursor.bufferedSize() > 0)
												state = 0;
											else
												state = 2;
										}
										break;
								case	2:
										List<Map<String,Object>> cpy = new ArrayList<Map<String,Object>>(buffer);
										buffer.clear();
										OnPlanChanged(cpy);
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
						if(mPlanCursor != null){
							try{ mPlanCursor.close(); } catch (Exception cl) { cl.printStackTrace(); }
						}
						mPlanCursor = null;
						SleepWait(5000);						
					}finally{
						if(mPlanCursor != null){
							try{ mPlanCursor.close(); } catch (Exception cl) { cl.printStackTrace(); }
						}
						mPlanCursor = null;
						state = 0;
					}			
				}
			}
        	

		});
        feedPlans.setName("FeedPlans");
        feedPlans.start();
        
        
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
					mSolver.solve();
				}
			}
		});
        t.setName("solver");
        t.start();
		
        
        
		
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
		        			SleepWait(1000);
		        		}
		        		
					}
	        		if(last != null){
	        			
	        			try {
	    					SerializationUtils.serialize(last, new FileOutputStream("solution_dbg.data"));
	    					//continue;
	    				} catch (IOException e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
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
	        			lock.put("time", System.currentTimeMillis());
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
		        				
		        				Score other = Score.parseScore((String) e.get("score"));
		        				int oVersion = Integer.parseInt(e.get("version").toString());
		        				int status = e.containsKey("status") ? Integer.parseInt(e.get("status").toString()) : 0;
		        				long time = ((Long)e.get("time"));
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
	        				//continue;
	        			}
	        			
	    				getLogger().info("Saving new solution  : "+last.getScore());
	    				
	        			lock.insert("lock");
	        			String planner_id = getPlannerUUID();
	        			
	        			long update_counter = 0;
	        			try{
		        			HashMap<String, Object> maxUpdatePlan = DbRethinkMap.GetMax("plan", "update_counter");
		        			if(maxUpdatePlan != null){
		        				update_counter = (long) maxUpdatePlan.getOrDefault("update_counter", 0);
		        			}
	        			}catch(com.rethinkdb.gen.exc.ReqlNonExistenceError ne){
	        				ne.printStackTrace();
	        			}
	        			update_counter++;
	        			
	        			List<Operation> operations = last.getOperationList(); //new ArrayList<Operation>(); //
	        			
	        			
	        			//calculate working groups
	        			List<Resource> res = last.getResourceList();
	        			SortedArrayList<Operation> sorted = new SortedArrayList<>(new Comparator<Operation>() {
	        				@Override
	        				public int compare(Operation o1, Operation o2) {
	        					return o1.getStartDate().compareTo(o2.getStartDate());
	        				}
	        			});
	        			Map<Operation, String> operationAssignmentGroups = new HashMap<>();
	        			List<String> usedAssignmentGroups = new ArrayList<>();
	        			for(Resource r : res){
	        				Set<Operation> rops = r.getOperationList();
	        				sorted.clear();
	        				sorted.addAll(rops);
	        				StockItem lastIoi = null;
	        				String assignment_group = "";
	        				
	        				for(Operation o : sorted){
	        					StockItem ioi = null;
	        					for(StockItemTransaction tr : o.getWorkOrder().getRequiredTransaction()){
	        						StockItem itm = tr.getItem();
	        						if("1550".equals(itm.getTsicod(0)) || "4550".equals(itm.getTsicod(0))){
	        							ioi = itm;
	        							break;
	        						}
	        					}
	        					
	        					
	        					if(ioi != null){
	        						if(lastIoi == null || !lastIoi.getReference().equals(ioi.getReference())){
	        							assignment_group = ioi.getReference();
	        							String un = assignment_group;
	        							int n = 0;
	        							do{
	        								un = assignment_group + "_" + n;
	        								n++;
	        							}while(usedAssignmentGroups.contains(un));
	        							assignment_group = un;
	        							usedAssignmentGroups.add(assignment_group);
	        						}
	        						operationAssignmentGroups.put(o, assignment_group);
	        					}else{
	        						assignment_group = "";
	        					}
	        					
	        					
	        					lastIoi = ioi;
	        				}
	        				
	        				
	        				
	        			}
	        			usedAssignmentGroups.clear();
	        			usedAssignmentGroups = null;
	        			
	        			
	        			
	        			
	        			
	        			List<Map<String, Object>> batch_insert = new ArrayList<Map<String, Object>>(operations.size());
	        			//List<HashMap<String, Object>> batch_update = new ArrayList<HashMap<String, Object>>(operations.size());
	        			now = System.currentTimeMillis();
	        			long last_lock_update = System.currentTimeMillis();
	        			int counter = 0;
	        			int updates_count = 0;
	        			int inserts_count = 0;
	        			int deletes_count = 0;
	        			
	        			List<DbRethinkMap> knownPlans = DbRethinkMap.GetAll("plan",0, 100000, RethinkDB.r.hashMap("deleted", false));
	        			
						for(Operation o : operations){
							
							
							String assignment_group = operationAssignmentGroups.getOrDefault(o, "");
									
							
							if(counter % 100 == 0){
								getLogger().info("Saved "+counter+" plans of "+operations.size());
							}
							counter++;
							//OffsetDateTime start = OffsetDateTime.ofInstant(Instant.ofEpochSecond(o.getStartDate()), ZoneId.of("UTC"));
							//OffsetDateTime end = OffsetDateTime.ofInstant(Instant.ofEpochSecond(o.getEndDate()), ZoneId.of("UTC"));
							long start = o.getStartDate()*1000;
							long end = o.getEndDate()*1000;
							
							if((System.currentTimeMillis() - last_lock_update) > 60000){
								lock.put("time", System.currentTimeMillis());
								lock.Update("lock", lock.get("id").toString());
								last_lock_update = System.currentTimeMillis();
							}
							
							
							
							//Instant start = Instant.ofEpochSecond(o.getStartDate());
							//Instant end = Instant.ofEpochSecond(o.getEndDate());
							//long start = o.getStartDate()*1000;
							//long end = o.getEndDate()*1000;
							String mfgnum = o.getWorkOrder().getMFGNUM();
							//HashMap<String, Object> map = DbRethinkMap.Get("plan", RethinkDB.r.hashMap("mfgnum", mfgnum).with("operation", o.getCode()));
							//HashMap<String, Object> map = DbRethinkMap.Get("plan", "mfgnum", mfgnum);
							Map<String, Object> map = null;
							int i;
							for(i = 0; i < knownPlans.size(); i++){
								Map<String,Object> m = knownPlans.get(i);
								if(mfgnum.equals(m.get("mfgnum")) && o.getCode().equals(m.get("operation"))){
									map = m;
									break;
								}
							}
							if(map != null){
								knownPlans.remove(i);	//make it faster on next search
							}
							
							StockItemProductionTransaction itmTrans = o.getWorkOrder().getProducedTransactionList().get(0);
							String itmItmref = "";
							String itmItmdes = "";
							double qty = 0;
							String stu = "UN";
							String vcrnumOri = "";
							int vcrlinori = 0;
							if(itmTrans != null){
								itmItmref = itmTrans.getItem().getReference();
								itmItmdes = itmTrans.getItem().getDescription();
								qty = (double)itmTrans.getQuantity();
								stu = itmTrans.getSTU();
								vcrnumOri = itmTrans.getVCRNUMORI();
								vcrlinori = itmTrans.getVCRLINORI();
							}
							
							
							if(map == null){
								//create plan from scratch
								map = new DbRethinkMap();
								map.put("assigned",  false);
								map.put("assignment_code",  "");
								map.put("assignment_group",  assignment_group);
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
								map.put("duration",  o.getBaseDuration());
								map.put("deleted",  false);
								long updt = System.currentTimeMillis();
								map.put("update",  updt);
								map.put("update_planner",  updt);
								map.put("update_counter",  update_counter);
								map.put("operations_count",  o.getWorkOrder().getOperations().size());
								map.put("operations_done",  0);
								map.put("itmref",  itmItmref);
								map.put("itmdes",  itmItmdes);
								map.put("itmqty",  qty);
								map.put("itmstu",  stu);
								map.put("vcrnumori",  vcrnumOri);
								map.put("vcrlinori",  vcrlinori);
								map.put("cplqty",  o.getCplqty());
								map.put("extqty",  o.getExtqty());
								map.put("assigned_qty",  0);
								map.put("qsmode",  0);	//sync mode for qty 0 : client sync, 1: planner sync
								
								Operation next = o.getNextOperation();
								String nextmfgnum = "";
								int nextopenum = 0;
								if(next != null){
									nextmfgnum = next.getWorkOrder().getMFGNUM();
									nextopenum = next.getOPENUM();
								}
								
								map.put("nextmfgnum", nextmfgnum);
								map.put("nextopenum", nextopenum);
								batch_insert.add(map);
								
							}else{
								
								
								
								boolean success = false;
								while(!success && map != null){	//map could have been deleted midway
									
									Map<String, Object> oldMap = map;
									boolean assigned = (boolean)oldMap.getOrDefault("assigned", false);
									
									
									//if((boolean)oldMap.getOrDefault("assigned", false)){
									//	break;
									//}
								
									String id = (String)oldMap.get("id");
									//long update_counter = (Long)oldMap.get("update_counter");
									
									boolean changed = false;
									
									map = new DbRethinkMap();
									map.put("id", id);
									if(!planner_id.equals(oldMap.get("planner_id"))){
										map.put("planner_id", planner_id);
										changed = true;
									}
									//FIXED: need to ignore assignment groups in no planning mode
									/*
									if(!assignment_group.equals(oldMap.get("assignment_group"))){
										map.put("assignment_group", assignment_group);
										changed = true;
									}
									*/
									
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
									
									boolean wst_changed = false;
									
									if(!o.getResource().getWorkcenter().equals(oldMap.get("workcenter"))){
										map.put("workcenter", o.getResource().getWorkcenter());
										wst_changed = true;
									}
									
									if(!o.getResource().getCode().equals(oldMap.get("workstation"))){
										map.put("workstation", o.getResource().getCode());
										wst_changed = true;
									}
									if(o.getResource().getInstance() != (Long)oldMap.get("workstation_number")){
										map.put("workstation_number", o.getResource().getInstance());
										wst_changed = true;
									}
									
									if(wst_changed){
										//reset assignment or all is lost
										map.put("assigned", false);
										map.put("assigned_qty", 0);
										map.put("assignment_code", "");
										map.put("assignment_group", "");
										changed = true;										
									}
									
									
									//if(!start.equals(oldMap.get("start"))){
									//if(start != ((OffsetDateTime)oldMap.get("start")).toEpochSecond()){
									if(start != ((Long)oldMap.get("start"))){
										map.put("start",  start);
										//map.put("start", RethinkDB.r.epochTime(start));
										changed = true;
									}
									//if(!end.equals(oldMap.get("end"))){
									//if(end != ((Long)oldMap.get("end"))){
									if(end != ((Long)oldMap.get("end"))){
										map.put("end",  end);
										//map.put("end", RethinkDB.r.epochTime(end));
										changed = true;
									}
									
									if(o.getBaseDuration() != (Long)oldMap.get("duration")){
										map.put("duration",  o.getBaseDuration());
										changed = true;
									}
									
									if(!itmItmref.equals(oldMap.get("itmref"))){
										map.put("itmref", itmItmref);
										changed = true;
									}
									if(!itmItmdes.equals(oldMap.getOrDefault("itmdes", ""))){
										map.put("itmdes", itmItmdes);
										changed = true;
									}
									
									if(Math.abs(qty - ((Number)oldMap.get("itmqty")).doubleValue()) > 0.001){
										map.put("itmqty",  qty);
										changed = true;
									}
									
									if(!stu.equals(oldMap.get("itmstu"))){
										map.put("itmstu", stu);
										changed = true;
									}
									
									if(!vcrnumOri.equals(oldMap.get("vcrnumori"))){
										map.put("vcrnumori", vcrnumOri);
										changed = true;
									}
									
									if(vcrlinori != (long)oldMap.get("vcrlinori")){
										map.put("vcrlinori", vcrlinori);
										changed = true;
									}
									
									if(((Number)oldMap.getOrDefault("qsmode", 0)).intValue() == 1){
									
										//1 means clients get updated by X3 
										if(Math.abs(o.getExtqty() - ((Number)oldMap.get("extqty")).doubleValue()) > 0.001){
											map.put("extqty", o.getExtqty());
											changed = true;
										}
										if(Math.abs(o.getCplqty() - ((Number)oldMap.get("cplqty")).doubleValue()) > 0.001){
											map.put("cplqty", o.getCplqty());
											changed = true;
										}
									
									}
									
									
									Operation next = o.getNextOperation();
									String nextmfgnum = "";
									int nextopenum = 0;
									if(next != null){
										nextmfgnum = next.getWorkOrder().getMFGNUM();
										nextopenum = next.getOPENUM();
									}
									
									if(!nextmfgnum.equals(oldMap.get("nextmfgnum"))){
										map.put("nextmfgnum", nextmfgnum);
										changed = true;
									}
									if(nextopenum != ((Number)oldMap.get("nextopenum")).intValue()){
										map.put("nextopenum", nextopenum);
										changed = true;
									}
									
									long updt = System.currentTimeMillis();
									map.put("update",  updt);
									map.put("update_planner",  updt);
									map.put("update_counter",  update_counter);
									
									
									//boolean list_changed = false;
									
									//need to update them all so we can delete the ones that weren't touched
									//FIXED: look plan by plan not updated
									//changed = true;	//update time was set
									
									
									//batch_update.add(map);
									//if(!changed || DbRethinkMap.UpdateIf("plan", id, map, "update_counter", update_counter) > 0){
									if(!changed || DbRethinkMap.Update("plan", id, map ) > 0 ){
										if(changed)
											updates_count++;
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
							inserts_count = batch_insert.size();
							//DbRethinkMap.Update("plan", batch_update);
							batch_insert.clear();
						}
						int offset = 0;
						List<DbRethinkMap> oldOnes = null;
						while((oldOnes = DbRethinkMap.GetAllLT("plan", offset, 10000, RethinkDB.r.hashMap("deleted", false), "update_counter", update_counter)) != null && oldOnes.size() > 0){
							offset += oldOnes.size();
							for(Map<String,Object> m : oldOnes){
								Operation ex = null;
								for(Operation o : operations){
									if(o.getWorkOrder().getMFGNUM().equals(m.get("mfgnum"))){
										if(o.getOPENUM() == (Long)m.get("openum")){
											ex = o;
											break;
										}
									}
								}
								if(ex == null){
									long updt = System.currentTimeMillis();
									//DbRethinkMap.Update("plan", m.get("id").toString(), RethinkDB.r.hashMap("deleted", true).with("update", updt).with("update_planner", updt));
									DbRethinkMap.Update("plan", m.get("id").toString(), RethinkDB.r.hashMap("deleted", true).with("update", updt).with("update_planner", updt));
									//DbRethinkMap.Delete("plan", m.get("id").toString());
									deletes_count++;
								}
								
							}
						}
						//purge deleted records more than a month old
						int c = DbRethinkMap.DeleteIfLT("plan", RethinkDB.r.hashMap("deleted", true), "update", System.currentTimeMillis() - 15*24*3600*1000L);
						getLogger().info("Purged "+c+" old records");
						
						lock.put("inserts", inserts_count);
						lock.put("updates", updates_count);
						lock.put("deletions", deletes_count);
						lock.put("status", 0);
						lock.Update("lock", lock.get("id").toString());
						
						getLogger().info(String.format("Finished updating plan : %d inserts, %d updates, %d deletions", inserts_count, updates_count, deletes_count));

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
			int count = Integer.parseInt(m.get("count").toString());
			for(int c = 0; c < count; c++){
				Resource r = null;
				r = new Resource();
				r.setCapacity((int)(Float.parseFloat(m.get("capacity").toString())*3600));
				r.setCode(m.get("wst").toString());
				r.setWorkcenter(m.get("wcr").toString());
				r.setInstance(c);
				r.setId(String.format("%s/%s-%d", r.getWorkcenter(), r.getCode(), r.getInstance()));
				
				//note that Calendar.SUNDAY = 1
				int[] dailyCap = new int[]{ 
						(int)(Float.parseFloat(m.get("capacity_sun").toString())*3600),
						(int)(Float.parseFloat(m.get("capacity_mon").toString())*3600),
						(int)(Float.parseFloat(m.get("capacity_thu").toString())*3600),
						(int)(Float.parseFloat(m.get("capacity_wed").toString())*3600),
						(int)(Float.parseFloat(m.get("capacity_tue").toString())*3600),
						(int)(Float.parseFloat(m.get("capacity_fri").toString())*3600),
						(int)(Float.parseFloat(m.get("capacity_sat").toString())*3600)
				};
				r.setDailyCapacity(dailyCap);
				
				//r.setOperationList(new ArrayList<Operation>());
				res.add(r);
			}
		}
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
				map.Update("workorder", wo.get("MFGNUM").toString());
			}
		}
		
	}
	
	protected int loadWorkOrders(){
		int res = 0;
		//List<DbRethinkMap> workorders = DbRethinkMap.GetAll("workorder", 0, 20000);
		//HashMap<String,String> filter = new HashMap<>();
		//filter.put("status", "MOD");
		//List<DbRethinkMap> workorders = DbRethinkMap.GetAll("workorder",0, 20000, filter); 
		//List<DbRethinkMap> workorders = DbRethinkMap.GetAllLT("workorder",0, 50000, null, "mfgtrkflg", 5);
		List<DbRethinkMap> workorders = DbRethinkMap.GetRunningWO("workorder", 0, 500000);
		
		if(workorders.size() >= 500000){
			getLogger().error("Maximum supported OFs reached");
		}
		
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
		for(Map<String,Object> w : list){
			mWorkorderLoadedUpdateMark = Math.max(((Number)w.getOrDefault("update", 0L)).longValue(), mWorkorderLoadedUpdateMark);
		}
		
		OnWorkOrderChanged(list);
		
		//test(dummy);
		
		res = list.size();
		/*
		for(Map<String,Object> wo : workorders){
			OnWorkOrderChanged(dummy, wo);
        	res++;
		}
		*/
		return res;
	}
	
	
	
	protected void OnWorkOrderChanged(final List<Map<String,Object>> wos) {
		
		boolean stock_item_list_changed = false;
    	boolean order_created = false;
    	boolean order_modified = false;
		Map<StockItem, Integer> referencedItems = new HashMap<>();
		
		

    	Schedule sch = mSolver.lockWorkingSolution();
    	try{
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
		    	
		    	for(WorkOrder wo : schOrderList){
		    		for(StockItemTransaction tr : wo.getProducedTransactionList())
		    			referencedItems.put(tr.getItem(), referencedItems.getOrDefault(tr.getItem(), 0) + 1);
		    		for(StockItemTransaction tr : wo.getRequiredTransaction())
		    			referencedItems.put(tr.getItem(), referencedItems.getOrDefault(tr.getItem(), 0) + 1);	    		
		    	}
		    	
				for(Map<String,Object> wo : wos){
					
					List<String> groups = (List<String>) wo.get("groups");
					

					
					
					mSolverBackoff = 0;
					//final Map<String,Object> wo
					String MFGNUM = wo.get("mfgnum").toString();
					String status = wo.get("status").toString();
					
					long MFGSTA = (Long)wo.getOrDefault("mfgsta", 1L);
			    	//long ENDDAT = ((OffsetDateTime)wo.get("ENDDAT")).getLong(ChronoField.INSTANT_SECONDS);
					//long ENDDAT = ((OffsetDateTime)wo.get("enddat")).toEpochSecond();
					long ENDDAT = ((long)wo.get("enddat"))/1000;
					//long STRDAT = ((OffsetDateTime)wo.get("strdat")).toEpochSecond();
					long STRDAT = ((long)wo.get("strdat"))/1000;
					long update = wo.containsKey("update") ? (long)wo.get("update") : 0;
					boolean deleted = (boolean)wo.getOrDefault("deleted", false);

					
					//FIXME: the following filters are used for the end of the beginning
					
					List<String> valid_workcenters = new ArrayList<String>();
					valid_workcenters.add("COR");
					valid_workcenters.add("PIN");
					valid_workcenters.add("QUI");
					valid_workcenters.add("SOL");
					valid_workcenters.add("TUB");
					
					boolean discard = true;
					for(String g : groups){
						if(g.startsWith("PJT_") && (  "PJT_201818".compareTo(g) < 0 || "PJT_ ".equals(g) || "PJT_201817B".equals(g) || "PJT_201817C".equals(g) ) && !"STAND BY".equals(g))
							discard = false;
						//if(g.startsWith("PJT_") &&  "PJT_201905A".equals(g) )
						//	discard = false;
						//if(g.startsWith("WCR_") &&  !valid_workcenters.contains(g))
						//	discard = true;
						
					}
					
					if(discard)
						continue;
					

					
					
					
					if(update > mLastChangeTime)
						mLastChangeTime = update;
		
					boolean deletemfg = deleted || MFGSTA >= 4 || "FIN".equals(status);
			    	
					if(!deletemfg){
						//take a peek at operations to check if all are satisfied
						List<Map<String, Object>> operations = (List<Map<String, Object>>) wo.get("operations");
						int count = 0;
						int completed = 0;
						for(Map<String, Object> op : operations){
							long opesta = (long)op.getOrDefault("opesta", 1L);
							if(opesta != 6){
								count++;
								long mfotrkflg = (long)op.getOrDefault("mfotrkflg", 1L);
								if(opesta >= 5 || mfotrkflg >= 5){
									completed++;
								}
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
				        		
				    			schOperationList.remove(o);
					    		
				        		Resource r = o.getResource();
				        		//Set<Operation> resOps = new HashSet<>(r.getOperationList());
				        		//r.setOperationList(resOps);
				        		
				        		//scoreDirector.beforeProblemFactChanged(r);
				        		r.getOperationList().remove(o);
					    		//scoreDirector.afterProblemFactChanged(r);
					    		order_modified = true;
					    		
					    		getLogger().debug("removed all operations "+o.toString());
				    		}
				    		
				    		/*
				    		scoreDirector.beforeProblemFactRemoved(order);
				    		schOrderList.remove(order);
				    		scoreDirector.afterProblemFactRemoved(order);
				    		*/
				    		schOrderList.remove(order);
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
			        		schExecutionGroups.remove(blackList);
				        	order_modified = true;
				        	List<StockItem> items = new LinkedList<>();
				        	for(StockItemTransaction tr : order.getProducedTransactionList()){
				        		items.add(tr.getItem());
				        	}
				        	for(StockItemTransaction tr : order.getRequiredTransaction()){
				        		items.add(tr.getItem());
				        	}
				        	
				    		for(StockItem it : items){
				    			int r = referencedItems.getOrDefault(it, 0);
				    			if(r > 0){
				    				referencedItems.put(it, r - 1);
				    			}else{
				    				referencedItems.put(it, 0);
				    				schItemList.remove(it);
				    			}
				    		}
	
				        	//remove it from plans
				        	
				        	
				        	
				        	
				    	}
				    	//disable this to avoid duplicate records on edge case
				    	//when we are publishing a solution
				    	/*
				    	try {
							mPlansToDelete.put(MFGNUM);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				    	*/
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
			            		schExecutionGroups.add(exg);
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
			    	
			    	            	
			    	
			    	//FIXME load stock managment mode for article instead of this hack
			    	int max_vcrlinori = 0;
			    	
			    	List<Map<String, Object>> items = (List<Map<String, Object>>) wo.get("items");
			    	for(Map<String, Object> it : items){
			    		String VCRNUMORI = it.get("vcrnumori").toString();
			    		Integer VCRLINORI = Integer.parseInt(it.getOrDefault("vcrlinori", 0).toString());
			    		String ITMREF = it.get("itmref").toString();
			    		String ITMDES = it.get("itmdes1").toString();
			    		String VCRITMREF = it.get("vcritmref").toString();
			    		Float UOMEXTQTY = Float.parseFloat(it.get("uomextqty").toString());
			    		String STU = it.get("stu").toString();
			    		Integer ITMSTA = Integer.parseInt(it.getOrDefault("itmsta", "1").toString());
			    		
			    		max_vcrlinori = Math.max(VCRLINORI, max_vcrlinori);
			    		
			        	StockItem exs = null;
			        	exs = schItemList.stream().filter(s -> ITMREF.equals(s.getReference())).findFirst().orElse(null);
			        	
			        	if(exs == null){
			        		exs = new StockItem(ITMREF);
			        		exs.setInitialAmount(0);
			        		exs.setReference(ITMREF);
			        		exs.setDescription(ITMDES);
			        		schItemList.add(exs);
			        		try {
			        			if(!mItemsToReload.contains(ITMREF))
			        				mItemsToReload.put(ITMREF);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
			        		stock_item_list_changed = true;
			        		order_modified = true;
			        	}else{
			        		if(ITMDES != null && !ITMDES.equals(exs.getDescription())){
			        			exs.setDescription(ITMDES);	
			        		}
			        	}
			        	final StockItem aux = exs;
			        	StockItemProductionTransaction trans = producedItems.stream().filter(i -> aux.equals(i.getItem())).findFirst().orElse(null);
			        	if(ITMSTA >= 4){
			        		//remove this one
			        		if(trans != null){
			        			producedItems.remove(trans);
			        			order_modified = true;
			        			assert(producedItems.size() > 0);
			        		}
			        		
			        	}else if(trans == null){
		        		
				        	trans = new StockItemProductionTransaction();
				        	trans.setItem(exs);
				        	trans.setQuantity(UOMEXTQTY);
				        	trans.setVCRNUMORI(VCRNUMORI);
				        	trans.setVCRLINORI(VCRLINORI);
				        	trans.setVCR(VCRNUMORI+"/"+VCRLINORI);
				        	trans.setSTU(STU);
				        	producedItems.add(trans);
				        	order_modified = true;
			        	}else{
			        		if(Math.abs(UOMEXTQTY - trans.getQuantity()) > 0.01){
			        			trans.setQuantity(UOMEXTQTY);
			        			order_modified = true;
			        		}
			        	}
			        	long max_soh_span = 15*24*3600;
			        	if(ENDDAT > 0){
			        		long endOfDay = (ENDDAT/(24*3600))*24*3600 + 18*3600; 
			        		ExecutionGroup inv = new ExecutionGroup("MFG_"+MFGNUM);
		        			inv.setEndTime(endOfDay);
		        			
		        			if(ITMREF.equals(VCRITMREF)){
		        				//the item beign ordered is required 1/2 day before
		        				inv.setEndTime(endOfDay - (long)((18-12.5)*3600));
		        				//the shipping must happen the same day (we actually consider since 00:00 AM to have some slack)
		        				long strdat = Math.min(24*3600*STRDAT/(24*3600), 24*3600*ENDDAT/(24*3600)); // + 8*3600;
			        			inv.setStartTime(strdat);
			        			
				        		for(ExecutionGroup g : executionGroups){
				        			if(g.getCode().startsWith("INV_")){	//TODO: should start with VCR
				        				g.setEndTime(inv.getEndTime());
				        				g.setStartTime(inv.getEndTime() - max_soh_span);
				        			}
				        		}
				        		
				        		//executionGroups.add(inv);
			            		//schExecutionGroups.add(inv);
			        			
			        			
		        			}else{
		        				//long strdat = (24*3600*ENDDAT/(24*3600)) - max_soh_span + 8*3600; //15 days before, tops
			        			//inv.setStartTime(strdat);
		        			}
		        			
		        			//sometimes wo date is after the order 
		        			//like generated by multilevel plannning a few days after the order date
		        			//so we don't lock the WO
		        			executionGroups.add(inv);
		            		schExecutionGroups.add(inv);
		        			
		        			
		        			
		            		/*
			        		if(ITMREF.equals(VCRITMREF)){
				        		inv = new ExecutionGroup("TRM_"+VCRNUMORI+"/"+VCRLINORI);
			        			inv.setEndTime(endOfDay);
			        			long strdat = Math.min(24*3600*STRDAT/(24*3600), 24*3600*ENDDAT/(24*3600));
			        			inv.setStartTime(strdat);
			        			executionGroups.add(inv);
			            		schExecutionGroups.add(inv);
			        		}	   
			        		*/
			        	}
			        	
			        	
			        	
			        	
			        	
			    	}
			    	
			    	
			    	order.setExecutionGroups(executionGroups);//force recalculation of start/end times
			    	
			    	//get me components            	
			    	List<Map<String, Object>> components = (List<Map<String, Object>>) wo.get("components");
			    	for(Map<String, Object> cmp : components){
			    		String ITMREF = cmp.get("itmref").toString();
			    		Float AVAQTY = Float.parseFloat(cmp.get("avaqty").toString());	//available
			    		Float RETQTY = Float.parseFloat(cmp.get("retqty").toString());	//required
			    		String STU = cmp.get("stu").toString();
			    		Integer MATSTA = Integer.parseInt(cmp.getOrDefault("matsta", "1").toString());
			    		
			    		
			        	StockItem exs = null;
			        	exs = schItemList.stream().filter(s -> ITMREF.equals(s.getReference())).findFirst().orElse(null);
			        	if(exs == null){
			        		exs = new StockItem(ITMREF);
			        		exs.setInitialAmount(AVAQTY);
			        		exs.setReference(ITMREF);
			        		schItemList.add(exs);
			        		try {
			        			if(!mItemsToReload.contains(ITMREF))
			        				mItemsToReload.put(ITMREF);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
			        		stock_item_list_changed = true;
			        		order_modified = true;
			        	}
			        	
			        	final StockItem aux = exs;
			        	StockItemTransaction trans = consumedItems.stream().filter(i -> aux.equals(i.getItem())).findFirst().orElse(null);
			        	if(MATSTA >= 4){
			        		if(trans != null){
			        			consumedItems.remove(trans);
			        			order_modified = true;
			        			assert(consumedItems.size() > 0);
			        		}
			        	}else if(trans == null){
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
			    		String EXTWST = o.get("extwst").toString();
			    		String WCR = o.get("wcr").toString();
			    		String OPEUOM = o.get("opeuom").toString();
			    		String TIMUOMCOD = o.containsKey("timuomcod") ? o.get("timuomcod").toString() : "";
			    		int OPENUM = Integer.parseInt(o.get("openum").toString());
			    		int NEXOPENUM = Integer.parseInt(o.getOrDefault("nexopenum", "0").toString());
			    		float tim = Float.parseFloat( o.get("extopetim").toString() );
			    		int mfotrkflg = Integer.parseInt(o.get("mfotrkflg").toString());
			    		int OPESTA = Integer.parseInt(o.getOrDefault("opesta", "1").toString());
			    		double CPLQTY = Double.parseDouble( o.get("cplqty").toString() );
			    		double EXTQTY = Double.parseDouble( o.get("extqty").toString() );
			    		
			    		if(mfotrkflg >= 5 || OPESTA >= 5){
			    			//this operation is complete
			    			Operation ex = null;
			    			for(int i = orderOperations.size() - 1 ; i >= 0; i--){
			    				//if orderOperations is not empty then this must be an update
			    				//so resource must be set and ready to be used
			    				ex = orderOperations.get(i);
			    				if(ex.getOPENUM() == OPENUM){
			    					
			    					Resource r = ex.getResource();
			    					r.getOperationList().remove(ex);
			    					
			    					orderOperations.remove(i);
						        	
						        	assert(schOperationList.contains(ex));
						        	schOperationList.remove(ex);
						    		order_modified = true;
						    		getLogger().debug("removed operation "+ex.toString());
			    				}
			    			}
			    			continue;
			    		}
			    		
			    		if("1".equalsIgnoreCase(TIMUOMCOD)){
			    			getLogger().warn("TIMUOMCOD indicates HOURS ... this is likely an error so replacing by minutes in "+MFGNUM);
			    			TIMUOMCOD = "2";
			    		}
			    		
			    		int EXTOPETIM = (int) ("1".equalsIgnoreCase(TIMUOMCOD) ? tim*3600 : tim*60 );
			    		if(EXTOPETIM == 0){
			    			getLogger().warn("EXTOPETIM is 0 ... this is likely an error so replacing by 30s in "+MFGNUM);
			    			EXTOPETIM = 30;
			    		}
			    		
			    		//EXTOPETIM = 60; //TODO: debugging arround
			    		
			    		Object opestr = o.getOrDefault("opestr", System.currentTimeMillis());
			    		long start = System.currentTimeMillis()/1000;
			    		if(opestr instanceof OffsetDateTime){
			    			start = ((OffsetDateTime)o.get("opestr")).getLong(ChronoField.INSTANT_SECONDS);
			    		}else if(opestr instanceof Long){
			    			start = ((Long)opestr)/1000; 
			    		}
			    		
			    		//Long FRCSTR = o.get("FRCSTR") != null ? ((OffsetDateTime)o.get("FRCSTR")).getLong(ChronoField.INSTANT_SECONDS) : System.currentTimeMillis()/1000;
			    		//Long start = o.get("opestr") != null ? ((OffsetDateTime)o.get("opestr")).getLong(ChronoField.INSTANT_SECONDS) : (System.currentTimeMillis()/1000) + 2*PLANNING_WINDOW_OFFSET;
			    		//long start = ((Long)o.getOrDefault("opestr", System.currentTimeMillis()))/1000;
			    		
			    		//FIXME: we can only update the operation because we are not actually doing splits
			    		//otherwise this gets way trickier
			    		Operation ex = null;
				    	ex = orderOperations.stream().filter(op -> OPENUM == op.getOPENUM()).findFirst().orElse(null);
			    		
				    	if(ex != null){
				    		//operation exists, update fields
				    		if(OPENUM != ex.getOPENUM())
				    			ex.setOPENUM((int)OPENUM);
				    		if(NEXOPENUM != ex.getNEXOPENUM())
				    			ex.setNEXOPENUM(NEXOPENUM);
				    		if(EXTOPETIM != ex.getBaseDuration())
				    			ex.setDuration(EXTOPETIM);
				    		
				    		if(Math.abs(EXTQTY - ex.getExtqty()) > 0.01 )
				    			ex.setExtqty(EXTQTY);
				    		if(Math.abs(CPLQTY - ex.getCplqty()) > 0.01 )
				    			ex.setCplqty(CPLQTY);
				    		
				    		boolean en = max_vcrlinori > 0 && valid_workcenters.contains(ex.getResource().getWorkcenter());
				    		if(en != ex.isDurationEnabled()){
				    			ex.setDurationEnabled(en);
				    		}
				    		Resource res = ex.getResource();
				    		if(!WCR.equals(res.getWorkcenter()) || !EXTWST.equals(res.getCode())){
				    			res.getOperationList().remove(ex);
		    					orderOperations.remove(ex);
		    					//schOperationList.remove(ex);
		    					
					    		List<Resource> oResourceList = sch.getResourceList().stream().filter(r -> EXTWST.equals(r.getCode()) && WCR.equals(r.getWorkcenter())).collect(Collectors.toList());
					    		res = oResourceList.get(0);
					    		ex.setResource(res);
					        	ex.setResourceRange(oResourceList);
				    			res.getOperationList().add(ex);
		    					orderOperations.add(ex);
		    					//schOperationList.add(ex);
		    					
					    		order_modified = true;
					    		getLogger().debug("removed workstation "+ex.toString());
				    		}
				    		
				    		continue;
				    	};
			    		
			    		
			    		if(orderOperations.stream().anyMatch(op -> OPENUM == op.getOPENUM())){	//the untouched code allways exists so we can check for the original code
			    			continue;
			    		}
			    		//bellow this point it is a new operation
			    		
			    		
			    		//List<Resource> oResourceList = new ArrayList<Resource>();
			    		List<Resource> oResourceList = sch.getResourceList().stream().filter(r -> EXTWST.equals(r.getCode()) && WCR.equals(r.getWorkcenter())).collect(Collectors.toList());
			    		//oResourceList = new ArrayList<>(oResourceList);
			    		Resource res = oResourceList.get(0);
			    		Set<Operation> resourceOps = res.getOperationList();	//what's the point of clone nowadays
			    		//Set<Operation> resourceOps = new HashSet<Operation>(res.getOperationList());
			    		//res.setOperationList(resourceOps);
			    		int counter = 0;
	//		    		boolean isMovable = true;
	//		    		if("EXPE".equals(res.getCode())){
	//		    			isMovable = false;
	//		    			start 
	//		    		}
			    		
			    		//disable spliting for now... having trouble with nextmfgnum/nextopenum in plan			    		
			    		int max_opetim = Integer.MAX_VALUE; // 2*3600;
			    		int splits = 1 + EXTOPETIM / max_opetim;
			    		double qtd_per_split = EXTQTY / (EXTOPETIM * 1.0); 	//parts per second
			    		qtd_per_split = 2*EXTQTY; // (int) (qtd_per_split * max_opetim);	//max qty per split
			    		
			    		do{
			    			//TODO: if we split the ops we must split qts
			    		
			    			int scheduledTime = Math.min(EXTOPETIM, max_opetim);	//limit to 2 hours batch maximum
			    			if(scheduledTime < max_opetim*0.2){
			    				//this would mean we have a very small second split..
			    				//just allow it instead 
			    				scheduledTime = EXTOPETIM;
			    				qtd_per_split = EXTQTY;			    			
			    			}
			    			String code = MFGNUM + "." + OPENUM + (counter > 0 ? "-" + counter : "");
				        	Operation op = new Operation(code);
			        		boolean en = max_vcrlinori > 0 && valid_workcenters.contains(res.getWorkcenter());
			        		op.setDurationEnabled(en);
				        	//op.setCode(counter == 0 ? id : id + "."+counter);
				        	op.setCode(code);
				        	op.setOPENUM((int)OPENUM);
				        	op.setNEXOPENUM(NEXOPENUM);
				        	op.setDuration(scheduledTime);
				        	op.setResource(res);
				        	op.setResourceRange(oResourceList);
				        	op.setResourceRequirement(scheduledTime);
				        	op.setStartDate(start);
				        	//op.setMaxStartDate(start);
				        	op.setWorkOrder(order);
				        	
			        		//scoreDirector.beforeProblemFactChanged(res);
			        		resourceOps.add(op);
				    		//scoreDirector.afterProblemFactChanged(res);

			        		op.setExtqty(Math.min(qtd_per_split, EXTQTY));
			        		op.setCplqty(Math.min(qtd_per_split, CPLQTY));
			        		EXTQTY -= op.getExtqty();
			        		CPLQTY -= op.getCplqty();
				        	
			        		assert(EXTQTY >= 0);
			        		assert(CPLQTY >= 0);
			        		
				        	
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
				        	
				        	assert(!schOperationList.contains(op));
				        	schOperationList.add(op);
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
				    	
				    	schOrderList.add(order);
				    	order.setFirstStart(firstStart == null ? System.currentTimeMillis()/1000 : firstStart);
				    	order.setLastEnd( lastEnd == null ? System.currentTimeMillis()/1000 : lastEnd);
				    	
				    	/*
				    	scoreDirector.beforeProblemFactAdded(order);
				    	schOrderList.add(order);
			    		scoreDirector.afterProblemFactAdded(order);
				    	*/
				    	
			    	}else if(order_modified){
			    		
				    	order.setFirstStart(firstStart == null ? System.currentTimeMillis()/1000 : firstStart);
				    	order.setLastEnd( lastEnd == null ? System.currentTimeMillis()/1000 : lastEnd);
			    		
			    	}
			    	
			    	for(WorkOrder w : schOrderList){
			    		List<ExecutionGroup> gs = w.getExecutionGroups();
			    		w.setExecutionGroups(gs); //recalculate min max dates
			    	}
			    	
				}
	    	}
    	}finally{
    		getLogger().debug("releasing working solution, changes = "+(order_created || order_modified));
    		mSolver.releaseWorkingSolution(order_created || order_modified);
    	}
    	
    	
    	if(stock_item_list_changed){
    		
    		synchronized (mFeedSync) {
    			if(mItemStockCursor != null)
    				mItemStockCursor.close();	//cause reload on the feed
			}
    		
    	}
    	
	}
	
	
	
	
	
	
	protected int loadStockItems(){
		int res = 0;
		Schedule sch = mSolver.lockWorkingSolution();
		try{
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
			
			OnStockItemChanged(list);
			res = list.size();
		}finally{
    		mSolver.releaseWorkingSolution();
    	}
		return res;
	}
	
	
	protected void OnStockItemChanged(final List<Map<String,Object>> items) {
		boolean changes = false;
    	Schedule sch = mSolver.lockWorkingSolution();    
    	try{
	    	// A SolutionCloner does not clone problem fact lists (such as executionGroups)
	    	// Shallow clone the executionGroups so only workingSolution is affected, not bestSolution or guiSolution
	    	synchronized (sch.getStockItemList()) {
		    	List<StockItem> schStockItems = new ArrayList<StockItem>(sch.getStockItemList()) ;
		    	sch.setStockItemList( schStockItems );
				for(Map<String,Object> item : items){
			
					String itmref = item.get("itmref").toString();
					String tclcode = item.containsKey("tclcode") ? item.get("tclcode").toString() : "";
					String tsicod0 = item.containsKey("tsicod0") ? item.get("tsicod0").toString() : "";
					Float physto = Float.parseFloat(item.get("physto").toString());
					long update = item.containsKey("updstp") ? (long)item.get("updstp") : 0;
					if(update > mLastChangeTime)
						mLastChangeTime = update;
	
					long reocod = item.containsKey("reocod") ? (long)item.get("reocod") : 3;	//3 - manufacture
		    	
			    	StockItem exs = null;
			    	exs = schStockItems.stream().filter(s -> itmref.equals(s.getReference())).findFirst().orElse(null);
			    	
			    	if(exs != null){
			    		if(!tclcode.equals(exs.getCategory())){
			    			exs.setCategory(tclcode);
			    			changes = true;
			    		}
			    		if(!tsicod0.equals(exs.getTsicod(0))){
			    			exs.setTsicod(0, tsicod0);
			    			changes = true;
			    		}
			    		if(reocod != exs.getReocod()){
			    			exs.setReocod((int)reocod);
			    			changes = true;
			    		}
			    		if((Math.abs(exs.getInitialAmount() - physto) > 0.001)){
	//			    		scoreDirector.beforeProblemFactChanged(exs);
				    		exs.setInitialAmount(physto);
				    		
	//			    		scoreDirector.afterProblemFactChanged(exs);
				    		//changes = true;
			    		}
			    	}
		    	}
			}
    	}finally{
    		mSolver.releaseWorkingSolution(changes);
    	}
    	

	}
	
	protected int loadCurrentPlans(){
		int res = 0;
		Schedule sch = mSolver.lockWorkingSolution();
		List<DbRethinkMap> plans = DbRethinkMap.GetAll("plan", 0, -1, RethinkDB.r.hashMap("deleted", false));
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(plans);
		
		OnPlanChanged(list);
		res = list.size();
		mSolver.releaseWorkingSolution();
		return res;
	}
	
	protected void OnPlanChanged(final List<Map<String,Object>> items) {
		
		boolean mods = false;
    	Schedule sch = mSolver.lockWorkingSolution();   	
    	try{
	    	// A SolutionCloner does not clone problem fact lists (such as executionGroups)
	    	// Shallow clone the executionGroups so only workingSolution is affected, not bestSolution or guiSolution
	    	List<Operation> schOperations = new ArrayList<Operation>(sch.getOperationList()) ;
	    	sch.setOperationList( schOperations );
			
			for(Map<String,Object> item : items){
			
				boolean assigned = (boolean)item.get("assigned");
				boolean completed = (boolean)item.get("completed");
				boolean deleted = (boolean)item.getOrDefault("deleted", false);
				
				
				if(deleted)
					continue;
				
				long start = ((Long) item.get("start"))/1000;
				//long start = ((OffsetDateTime) item.get("start")).toEpochSecond();
				//long end = ((OffsetDateTime) item.get("end")).toEpochSecond();
				//long duration = (Long) item.get("duration");
				String workcenter = (String) item.get("workcenter");
				String workstation = (String) item.get("workstation");
				long workstation_number = (Long) item.get("workstation_number");
				String plannerId = item.getOrDefault("planner_id", "").toString();
				double cplqty = ((Number)item.getOrDefault("cplqty", 0)).doubleValue();
				double extqty = ((Number)item.getOrDefault("extqty", 0)).doubleValue();
				
				String mfgnum = item.get("mfgnum").toString();
				String operation = item.get("operation").toString();
				
		    	Operation ex = null;
		    	ex = schOperations.stream().filter(o -> (mfgnum.equals(o.getWorkOrder().getMFGNUM()) && operation.equals(o.getCode()))).findFirst().orElse(null);
		    	
		    	if(ex == null){
		    		getLogger().error("Invalid plan change received...");
		    		continue;
		    	}
		    	
		    	//TODO: rethink this moveable thing... 
		    	//when operator selects more than one plan, they get the same start and therefore overlapp
		    	boolean moveable = true; // !(assigned || completed);
				//if(assigned || !mPlannerUUID.equals(plannerId))
		    	ex.setMovable(moveable);
	
		    	
		    	if(ex.getStartDate() != start){
		    		//scoreDirector.beforeVariableChanged(ex.getWorkOrder(), "firstStart");	//just make it recalculate
		    		ex.setMovable(true);
		    		mSolver.doMove(ex, start);
		    		ex.setMovable(moveable);
		    		//ex.setStartDate(start);
		    		
		    		//(new OperationDateUpdatingVariableListener()).afterVariableChanged(scoreDirector, ex);
		    		//scoreDirector.afterVariableChanged(ex.getWorkOrder(), "firstStart");
		    		
			    	//order.setFirstStart(firstStart == null ? System.currentTimeMillis()/1000 : firstStart);
			    	//order.setLastEnd( lastEnd == null ? System.currentTimeMillis()/1000 : lastEnd);
		    		mods = true;
	    		}
		    	if(Math.abs(cplqty - ex.getCplqty()) > 0.001){
		    		ex.setCplqty(cplqty);
		    		mods = true;
		    	}
		    	if(Math.abs(extqty - ex.getExtqty()) > 0.001){
		    		ex.setExtqty(extqty);
		    		mods = true;
		    	}
	    		
	    		
//				Resource res = sch.getResourceList().stream().filter(r -> (workcenter.equals(r.getWorkcenter()) && workstation.equals(r.getCode()) && (workstation_number == r.getInstance()))).findFirst().orElse(null);
//				if(res != null){
//					if(!res.equals(ex.getResource())){
//						//plan says res but wo says ex , FIXED: plan says sh*t
//						assert(ex.getResourceRange().contains(res));
//						
//						ex.setMovable(true);
//			    		mSolver.doMove(ex, res);
//			    		ex.setMovable(moveable);
//						
//						//ex.getResource().getOperationList().remove(ex);
//			    		//ex.setResource(res);
//			    		//res.getOperationList().add(ex);
//			    		mods = true;
//					}
//				}else{
//					getLogger().error("Unable to find resource...");
//				}
		    	
			}
    	}finally{
    		//mSolver.releaseWorkingSolution(mods);
    		//ignore all changes and make them runtime able
    		//otherwise we would spend all our time recalculating
    		mSolver.releaseWorkingSolution(false);	
    	}
		
    	
	}
	
	/**
	 * Returns an <code>InetAddress</code> object encapsulating what is most likely the machine's LAN IP address.
	 * <p/>current
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
	
	private static BaseSolver newSolver(Schedule schedule){
    	return new NoSolver(schedule);
    }
	
	public static void main(String[] args) throws InterruptedException{
		
		
		DbRethinkMap.setParameters("sig.sotubo.pt", 28015, "sotubo",  "");
		//DbRethinkMap.setParameters("192.168.10.240", 28015, "planner",  "");
		
		
		Planner<NoSolver> p = new Planner<NoSolver>();
		
		Thread t = new Thread(p);
		
		
		t.start();
		
		t.join();
		
	}
	
}
