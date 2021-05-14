package pt.sotubo.planner.solver;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.print.DocFlavor.INPUT_STREAM;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.encog.Encog;
import org.encog.engine.network.activation.ActivationElliott;
import org.encog.engine.network.activation.ActivationFunction;
import org.encog.engine.network.activation.ActivationLOG;
import org.encog.engine.network.activation.ActivationLinear;
import org.encog.engine.network.activation.ActivationRamp;
import org.encog.engine.network.activation.ActivationReLU;
import org.encog.engine.network.activation.ActivationSigmoid;
import org.encog.engine.network.activation.ActivationSoftMax;
import org.encog.engine.network.activation.ActivationTANH;
import org.encog.ml.data.MLData;
import org.encog.ml.data.MLDataPair;
import org.encog.ml.data.MLDataSet;
import org.encog.ml.data.basic.BasicMLData;
import org.encog.ml.data.basic.BasicMLDataPair;
import org.encog.ml.data.basic.BasicMLDataSet;
import org.encog.ml.train.MLTrain;
import org.encog.neural.error.ErrorFunction;
import org.encog.neural.networks.BasicNetwork;
import org.encog.neural.networks.layers.BasicLayer;
import org.encog.neural.networks.training.propagation.Propagation;
import org.encog.neural.networks.training.propagation.TrainingContinuation;
import org.encog.neural.networks.training.propagation.back.Backpropagation;
import org.encog.neural.networks.training.propagation.resilient.ResilientPropagation;
import org.encog.neural.networks.training.propagation.sgd.StochasticGradientDescent;
import org.encog.neural.networks.training.propagation.sgd.update.MomentumUpdate;
import org.encog.persist.EncogDirectoryPersistence;
import org.encog.util.simple.EncogUtility;
import org.neuroph.core.NeuralNetwork;
import org.neuroph.core.data.DataSet;
import org.neuroph.nnet.MultiLayerPerceptron;
import org.neuroph.nnet.Perceptron;
import org.neuroph.nnet.learning.BackPropagation;
import org.neuroph.util.TransferFunctionType;

import com.googlecode.fannj.Fann;
import com.googlecode.fannj.Layer;
import com.googlecode.fannj.Trainer;
import com.googlecode.fannj.TrainingAlgorithm;
import com.googlecode.fannj.WindowsFunctionMapper;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

import pt.sotubo.planner.Planner;
import pt.sotubo.planner.SortedArrayList;
import pt.sotubo.planner.domain.ExecutionGroup;
import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Resource;
import pt.sotubo.planner.domain.Schedule;
import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemProductionTransaction;
import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;

public class RLSolver{

	private static final ExecutorService THREADPOOL = Executors.newFixedThreadPool(2);
	private static long theLastRankComputeTime = 0L;
	private static final int ACTION_STAND_STILL = 0;
	private static final int ACTION_MOVE_FORWARD = 1;
	private static final int ACTION_MOVE_BACKWARDS = 2;
	private static final int ACTION_STICK_NEXT = 3;
	private static final int ACTION_STICK_PREV = 4;
	private static final int ACTION_JUMP_NEXT = 5;
	private static final int ACTION_JUMP_PREV = 6;
	private static final int FEATURES_SIZE = 145;
	private static final int OUTPUT_SIZE = 7;
	private static final int SAMPLES_MAX_LENGTH = 2048;
	
	private Lock mBestScoreSync;
	private Score mBestScore;
	private Schedule mBestSchedule;
	private Schedule mWorkingSolution;
	private Lock mWorkingSolutionSync;
	private volatile boolean mWorkingSolutionChanged;
	private OperationIncrementalScoreCalculator mCalculator;
	
	private Map<CacheKey, Integer> groupRankCache = null;
	private Object groupRankCacheSync;
	private List<NewBestSolutionListener> mSolutionListeners;
	private volatile boolean mRunning;
	private Map<Operation, List<Long>> mLastMoves;
	private Random mRandom;
	private Map<String, Long> mOperationTiredness;
	private long mOperationTirednessMax;
	private List<Sample> mSamples;
	private List<Sample>[] mPreciousSamples;
	private BasicNetwork mANN;
	private StochasticGradientDescent mTrainer;
	private File networkFile = new File("planner_greedy.nnet");
	//private NeuralNetwork mNeuroph;
	private ExFann mFann;
	private Trainer mFannTrainer;
	
	
	protected void setBestSchedule(Score score, Schedule sch){
		mBestScoreSync.lock();
		try{
			mBestScore = score;
			mBestSchedule = sch;
			mBestSchedule.setScore(score.multiply(1.0));
			System.out.println(Thread.currentThread().getId()+" > "+score.toString());
			
			for(NewBestSolutionListener l : mSolutionListeners){
				l.OnNewBestSolution(this, score);
			}
			
			
			
		}finally{
			mBestScoreSync.unlock();
		}
	}
	
	public Schedule getBestScheduleClone(){
		Schedule cl = null;
		mBestScoreSync.lock();
		try{
			if(mBestSchedule != null){
				cl = mBestSchedule.planningClone();
			}
		}finally{
			mBestScoreSync.unlock();
		}
		return cl;
	}
	
	public interface NewBestSolutionListener{
		void OnNewBestSolution(RLSolver solver, Score score);
	}
	public synchronized void addNewBestSolutionListener(NewBestSolutionListener listener){
		mSolutionListeners.add(listener);
	}
	
	
	public RLSolver(Schedule workingSolution) {
		mBestScore = null;
		mBestSchedule = null;
		mBestScoreSync = new ReentrantLock();
		mWorkingSolutionSync = new ReentrantLock(true);
		mWorkingSolution = workingSolution;
		mWorkingSolutionChanged = true;
		mCalculator = null;
		
		mSolutionListeners = new ArrayList<>();
		mRandom = new Random(System.currentTimeMillis());
		mLastMoves = new HashMap<>();
		mOperationTiredness = new HashMap<>();
		mOperationTirednessMax = 0;
		groupRankCache = null;
		groupRankCacheSync = new Object();
		mSamples = new ArrayList<>(SAMPLES_MAX_LENGTH);
		//mPreciousSamples = new ArrayList<>(SAMPLES_MAX_LENGTH);
		mPreciousSamples = new ArrayList[9];
		for(int i = 0; i<  mPreciousSamples.length; i++){
			mPreciousSamples[i] = new ArrayList<>();
		}
		
		System.setProperty("jna.library.path", "C:\\restore\\lib\\FANN-2.2.0-Source\\bin\\");
		System.out.println( System.getProperty("jna.library.path") ); //maybe the path is malformed
		File file = new File(System.getProperty("jna.library.path") + "fannfloat.dll");
		System.out.println("Is the dll file there:" + file.exists());
		System.load(file.getAbsolutePath());
		
		if(false && networkFile.exists()){
			
			mFann = new ExFann(networkFile.getAbsolutePath());
			
			//mANN = (BasicNetwork)(EncogDirectoryPersistence.loadObject(networkFile));
		}else{
			mANN = new BasicNetwork();
			mANN.addLayer(new BasicLayer(null,  true, FEATURES_SIZE));
			//mANN.addLayer(new BasicLayer(new ActivationReLU(),  true, 128));//(int) (FEATURES_SIZE * 3)));
			mANN.addLayer(new BasicLayer(new ActivationTANH(),  true, FEATURES_SIZE)); //(int) (FEATURES_SIZE * 2)));
			//mANN.addLayer(new BasicLayer(new ActivationReLU(),  true, OUTPUT_SIZE*2));
			//mANN.addLayer(new BasicLayer(new ActivationRamp(1e10, -1e10, 1e10, -1e10), false, OUTPUT_SIZE));
			mANN.addLayer(new BasicLayer(new ActivationTANH(), false, OUTPUT_SIZE));
			mANN.getStructure().finalizeStructure();
			mANN.reset();
		
			List<Layer> layers = new ArrayList<Layer>();
	        layers.add(Layer.create(FEATURES_SIZE));
	        layers.add(Layer.create(FEATURES_SIZE, com.googlecode.fannj.ActivationFunction.FANN_SIGMOID_SYMMETRIC));
	        layers.add(Layer.create(FEATURES_SIZE, com.googlecode.fannj.ActivationFunction.FANN_SIGMOID_SYMMETRIC));
	        layers.add(Layer.create(OUTPUT_SIZE*2, com.googlecode.fannj.ActivationFunction.FANN_SIGMOID_SYMMETRIC));
	        layers.add(Layer.create(OUTPUT_SIZE, com.googlecode.fannj.ActivationFunction.FANN_SIGMOID_SYMMETRIC));
	        mFann = new ExFann(layers);
			
		}
		mANN = null;
		
		
		mTrainer = null;
		
		
		
		
		
		
		//mNeuroph = new MultiLayerPerceptron(TransferFunctionType.SIGMOID, FEATURES_SIZE, FEATURES_SIZE, OUTPUT_SIZE);
		
        mFannTrainer = new Trainer(mFann);
        mFannTrainer.setTrainingAlgorithm(TrainingAlgorithm.FANN_TRAIN_INCREMENTAL);
        mFann.setLearningRate(0.6);
        mFann.setMomentum(0.3);
		
	}

	public void solve(){
		mRunning = true;
		run();
	}
	
	public void run() {
		
		mWorkingSolutionSync.lock();
		
		Schedule sch = mWorkingSolution;
		
		OperationIncrementalScoreCalculator calc = new  OperationIncrementalScoreCalculator();
		mCalculator = calc;
		boolean mod = false;
		int dbgCounter = 0;
		int iteration = 0;
		Calendar cal = Calendar.getInstance();
    	cal.setTimeZone(TimeZone.getTimeZone("GMT"));
    	SortedArrayList<Operation> sorted = new SortedArrayList<>(new Comparator<Operation>() {
			@Override
			public int compare(Operation o1, Operation o2) {
				return o1.getStartDate().compareTo(o2.getStartDate());
			}
		});
    	mWorkingSolutionChanged = true;
    	Score score = null;
    	Score lastScore = null;
    	List<Operation> ops = null;
    	Map<String, List<WorkOrder>> invoiceMap = null;
    	List<WorkOrder> allWo = null;
    	List<Resource> res = null;
    	//Map<Resource, List<Operation>> resourceOperationMap = null;
    	long updateCounter = 0;
    	long lastUpdateCounter = 0;
    	Map<Operation, Integer> opIndexes = new HashMap<>(10000);
    	Random mRandom = new Random();
    	//long lastModTime = System.currentTimeMillis();
    	int noModCounter = 0;
    	int iterationOpIndex = 0;
    	double[] featureNormalization = new double[FEATURES_SIZE]; 
    	long lastScoreIteration = 0;
    	double epsilon = 0.10;
    	long[] scoreValues;
    	long[] newScoreValues;
		do{
			mWorkingSolutionSync.unlock();
			Thread.yield();
			mWorkingSolutionSync.lock();
			
			if(mWorkingSolutionChanged || iteration == 0){	
				//on the first run we may be interrupted before getting to start
				//and we need to make sure shit gets initialized, hence iteration == 0
				
				
				
				ops = sch.getOperationList();
				invoiceMap = new HashMap<String, List<WorkOrder>>();
				allWo = sch.getWorkOrderList();
				for(WorkOrder wo  : allWo){
					for (StockItemProductionTransaction stockProduction : wo.getProducedTransactionList()) {
						List<WorkOrder> woList = invoiceMap.get(stockProduction.getVCR());
						if(woList == null){
							woList = new LinkedList<>();
							invoiceMap.put(stockProduction.getVCR(), woList);
						}
						woList.add(wo);
					}
					
					
					long min = Long.MAX_VALUE;
					long max = Long.MIN_VALUE;
					for(Operation op : wo.getOperations()){
						if(op.getStartDate() < min)
							min = op.getStartDate();
						if(op.getEndDate() > max)
							max = op.getEndDate();
					}
					if(min < Long.MAX_VALUE || max > Long.MIN_VALUE){
						//as long as there is one operation, there will be both min and max
						if(min != 1*wo.getFirstStart() || max != 1*wo.getLastEnd()){
							wo.setFirstStart(min);
							wo.setLastEnd(max);					
						}
					}
				}
				
				updateNextOperations(ops, allWo, invoiceMap);
				invoiceMap.clear();
				
				calc.resetWorkingSolution(sch);
				
				Map<String, Long> filteredOperations = new HashMap<>(mOperationTiredness);
				mOperationTiredness.clear();
				mOperationTirednessMax = 0;
				
				Map<Operation, List<Long>> filteredLastMoves = new HashMap<>(mLastMoves);
				mLastMoves.clear();
				
				res = sch.getResourceList();
//				resourceOperationMap = new HashMap<Resource, List<Operation>>();
//				for(Resource r : res){
//					resourceOperationMap.put(r, new ArrayList<Operation>());
//				}
				for(Operation o : ops){
					
					assert(o.getResource().getOperationList().contains(o));
//					assert(!resourceOperationMap.get(o.getResource()).contains(o));
//					resourceOperationMap.get(o.getResource()).add(o);
					Long v = filteredOperations.get(o.getCode());
					if(v != null){
						mOperationTiredness.put(o.getCode(), v);
						if(v > mOperationTirednessMax)
							mOperationTirednessMax = v;
					}
					
					List<Long> moves = filteredLastMoves.get(o);
					if(moves != null && moves.size() > 0){
						mLastMoves.put(o, moves);
					}
					
				}
				
				opIndexes.clear();
				for(int i = 0; i < ops.size() ; i++){
					opIndexes.put(ops.get(i), i);
				}
				mod = true;
				//groupRankCache = null;
				mWorkingSolutionChanged = false;
				
				score = calc.calculateScore();
				scoreValues = score.values();
				mBestScore = null; //score;	//ensure update after solution changes
				updateCounter = 0;
		    	lastUpdateCounter = 0;
		    	iterationOpIndex = 0;
		    	for(int i = 0; i < FEATURES_SIZE; i++)
		    		featureNormalization[i] = 1.0;
		    	
		    	lastScoreIteration = 0;
		    	
		    	if(iteration == 0){
		    		theLastRankComputeTime = System.currentTimeMillis();
		    		synchronized(groupRankCacheSync){
						groupRankCache = computeGroupRankCache(ops, theLastRankComputeTime);
					}
		    	}else{
		    	
		    		
			    	final List<Operation> cacheOps = sch.getOperationList();
			    	
					THREADPOOL.submit(new Runnable() {
						
						@Override
						public void run() {
							
							theLastRankComputeTime = System.currentTimeMillis();
							Map<CacheKey, Integer> cache = computeGroupRankCache(cacheOps, theLastRankComputeTime);
							if(cache != null){
								synchronized(groupRankCacheSync){
									groupRankCache = cache;
								}
							}
							
						}
					});
		    	}
			}
			
			
			
			iteration++;
			System.out.println("Starting iteration " + iteration + " -------------------------------- ");
			lastUpdateCounter = updateCounter;
			updateCounter = 0; 
			
			
			mod = false;
			
			//if(iteration % 10 == 0)
			{
				//mSamples.clear();
				iterationOpIndex++;
			}
				//iterationOpIndex = mRandom.nextInt(ops.size());
			//iterationOpIndex = mRandom.nextInt(ops.size());
			if(iterationOpIndex >= ops.size())
				iterationOpIndex = 0;
			//pick random operation
			Operation selectedOperation = ops.get(iterationOpIndex);
			Resource opResource = selectedOperation.getResource();
			sorted.clear();
			sorted.addAll(opResource.getOperationList());
			int opIndex = sorted.indexOf(selectedOperation);
			
			double[] features = features(selectedOperation, sorted, opIndex, ops);			
			double[] outs = new double[OUTPUT_SIZE];
			/*
			forward(features, outs);
			double max = -10;
			int autoSelected = 0;
			for(int i = 0; i < OUTPUT_SIZE; i++){
				if(outs[i] > max){
					max = outs[i];
					autoSelected = i;
				}
			}
			
			action(autoSelected, calc, selectedOperation, sorted, opIndex);
*/			
			

			long originalStart = selectedOperation.getStartDate();
			score = calc.calculateScore();
			scoreValues = score.values();
			long maxScore = Long.MIN_VALUE;
			int selectedAction = 0;
			long[] scores = new long[OUTPUT_SIZE];
			for(int a = 0; a < OUTPUT_SIZE; a++){
				action(a, calc, selectedOperation, sorted, opIndex);
				Score nScore = calc.calculateScore();
				long s = nScore.compareTo(score);
				scores[a] = s;
				if(s > maxScore){
					maxScore = s;
					selectedAction = a;
				}				
				doMove(calc, selectedOperation, originalStart);
			}
			
			
			
			forward(features, outs);
			double max = -10;
			int autoSelected = 0;
			for(int i = 0; i < OUTPUT_SIZE; i++){
				if(outs[i] > max){
					max = outs[i];
					autoSelected = i;
				}
			}
			//give preference to network output
			//if(scores[autoSelected] > 0.8)
			//	selectedAction = autoSelected;
			
			action(selectedAction, calc, selectedOperation, sorted, opIndex);
			sorted.remove(selectedOperation);
			sorted.add(selectedOperation);
			Score after  = calc.calculateScore();
			newScoreValues = after.values();
			
			if(iteration % 100 == 0){
				//decrease epsilon when we have more positive 
				int posCount = 0;
				int total = 0;
				int hits = 0;
				for(Sample s : mSamples){
					total++;
					if(s.reward == s.action)
						hits++;
				}
				double rate = hits/(1.0*total);			
				System.out.println("current hit rate : "+rate);
			}
			
			Sample s = new Sample();
			s.state = features;
			s.action = selectedAction;
			s.reward = autoSelected;
			//s.newState = features(selectedOperation, sorted, opIndex, ops);

			mSamples.add(s);
			if(mSamples.size() >= SAMPLES_MAX_LENGTH){
				mSamples.remove(mRandom.nextInt(mSamples.size()));				
			}
			
			int scoreIndex = -1;
			for(int i = 0; i < scoreValues.length; i++){
				if(newScoreValues[i] > scoreValues[i]){
					scoreIndex = i;
					break;
				}
			}
			if(scoreIndex >= 0){
				List<Sample> precious = mPreciousSamples[scoreIndex];
				precious.add(s);
				if(precious.size() >= SAMPLES_MAX_LENGTH){
					precious.remove(mRandom.nextInt(precious.size()));				
				}

			}
			//now the training part
			if(iteration % 100 == 0){
				List<Sample> shuffled = new LinkedList<>(mSamples);
				for(List<Sample> l : mPreciousSamples){
					for(Sample p : l){
						shuffled.add(mRandom.nextInt(shuffled.size()), p);
					}
				}
				train(shuffled);
				calc.resetWorkingSolution(sch);
			}

			
			
			
			dbgCounter = 0;
			/*
			if(selectedOperation.getNextOperation() != null && selectedOperation.getEndDate() > selectedOperation.getNextOperation().getStartDate()){
				Operation prev = opIndex > 0 ? sorted.get(opIndex - 1) : null;
				//Operation next = opIndex < sorted.size() - 1 ? sorted.get(opIndex + 1) : null;
				if(prev == null)
					selectedAction = ACTION_MOVE_BACKWARDS;
				else{
					switch(selectedAction){
						case	ACTION_MOVE_BACKWARDS:
						case	ACTION_STICK_PREV:
						case	ACTION_JUMP_PREV:
								break;
						default:
							if(prev.getEndDate() < selectedOperation.getStartDate())
								selectedAction = ACTION_STICK_PREV;
							else
								selectedAction = ACTION_JUMP_PREV;
							//selectedAction = ACTION_MOVE_BACKWARDS;
							break;
					}
				}
			}
			wo = selectedOperation.getWorkOrder();
			if(wo.getExecutionGroupForcedEnd() != null && selectedOperation.getEndDate() > wo.getExecutionGroupForcedEnd()){
				Operation prev = opIndex > 0 ? sorted.get(opIndex - 1) : null;
				//Operation next = opIndex < sorted.size() - 1 ? sorted.get(opIndex + 1) : null;
				if(prev == null)
					selectedAction = ACTION_MOVE_BACKWARDS;
				else{
					switch(selectedAction){
						case	ACTION_MOVE_BACKWARDS:
						case	ACTION_STICK_PREV:
						case	ACTION_JUMP_PREV:
								break;
						default:
							if(prev.getEndDate() < selectedOperation.getStartDate())
								selectedAction = ACTION_STICK_PREV;
							else
								selectedAction = ACTION_JUMP_PREV;
							//selectedAction = ACTION_MOVE_BACKWARDS;
							break;
					}
				}
			}
			//if(wo.getExecutionGroupForcedStart() != null && selectedOperation.getStartDate() < wo.getExecutionGroupForcedStart()){
			//	selectedAction = ACTION_MOVE_FORWARD;	
			//}
			
			*/
			
			
			
			
			
			
			
			
			
			
			
			
			//System.out.println(iteration + " moved " + dbgCounter + " operation on agent command ");
			
			
			
			
//			do{
//			
//				dbgCounter = 0;
//				//would actually need to this everytime we change it but...
//				sorted.clear();
//				sorted.addAll(ops);
//				
//				for(Operation o : ops){
//					
//					long startTime = o.getStartDate();
//					if(o.getNextOperation() != null){
//						long st = o.getNextOperation().getStartDate() - o.getDuration();
//						if(st < startTime)
//							startTime = st;
//					}
//					WorkOrder wo = o.getWorkOrder();
//					//if(!mod)
//					{
//						if(wo.getExecutionGroupForcedStart() != null && wo.getExecutionGroupForcedStart() > startTime){
//							
//							long fs = wo.getExecutionGroupForcedStart();
//							long end = o.getOverallEndDate();
//							long workingPeriod = (long)((end - fs)/(24*3600.0))*8*3600;
//							long span = o.getOverallDuration();
//							if(100*span < workingPeriod){	//otherwise it is quite unlikelly
//								
//								//sorted.clear();
//								//sorted.addAll(ops);
//								
//								
//								//startTime = wo.getExecutionGroupForcedStart();
//								int c = 0; //collapse(calc, sorted, o, -1);								
//								if(c > 0){
//										
//									mod = true;
//									updateCounter+=c;
//									dbgCounter+=c;
//									startTime = o.getStartDate();
//									
//								}
//							}
//							
//						}
//					}
//					
//					if(wo.getExecutionGroupForcedEnd() != null && wo.getExecutionGroupForcedEnd() < startTime + o.getDuration()){
//						long st = wo.getExecutionGroupForcedEnd() - o.getDuration();
//						if(st < startTime)
//							startTime = st;
//					}
//					
//					
//					//if(o.getStartDate() > startTime || (forcedStart && o.getStartDate() < startTime)){
//					if(o.getStartDate() != startTime){
//						if(doMove(calc, o, startTime)){
//							mod = true;
//							updateCounter++;
//							dbgCounter++;
//							sorted.remove(o);
//							sorted.add(o);
//						}
//					}
//					
//	
//				}
//				//score = calc.calculateScore();
//				//System.out.println(iteration + " Initialized "+dbgCounter+" on a time sequence step " + score);
//				System.out.println(iteration + " Initialized "+dbgCounter+" on a time sequence step ");
//				
//				dbgCounter = 0;
//				//for(Resource r : resourceOperationMap.keySet()){
//				for(Resource ro : res){
//					//List<Operation> rops = resourceOperationMap.get(r);
//					List<Operation> rops = new ArrayList<>(ro.getOperationList());
//					if(rops.size() < 2)
//						continue;
//					sorted.clear();
//					sorted.addAll(rops);
//					long maxEnd = Long.MAX_VALUE;// sorted.get(sorted.size() - 1).getStartDate();
//					for(int i = sorted.size() - 1; i >= 0; i--){
//						
//						Operation o = sorted.get(i);
//						long bestStart = i < sorted.size() - 1 ? sorted.get(i+1).getStartDate() - o.getDuration() : o.getStartDate();
//						if(o.getStartDate() > bestStart){	
//						//if(o.getStartDate() != maxStart){
//							
//							if(i < sorted.size() - 1){
//								Operation n = sorted.get(i+1);
//								if(bestStart >= n.getStartDate()){
//									log("WTF");
//								}
//							}
//							
//							if(doMove(calc, o, bestStart)){
//								mod = true;
//								dbgCounter  ++;
//								updateCounter++;
//							}
//						}
//					}
//				}
//				//score = calc.calculateScore();
//				//System.out.println(iteration + " Initialized " + dbgCounter + " on simultaneous operation step " + score);
//				System.out.println(iteration + " Initialized " + dbgCounter + " on simultaneous operation step ");
//			}while(false);
			
			score = calc.calculateScore();
			System.out.println(iteration + " Current score "+score);
			
			if(mBestScoreSync.tryLock()){
				try{
					//if(mBestScore == null || score.compareTo(mBestScore) > 0 || (updateCounter == 0 && lastUpdateCounter > 0))
					int compare = mBestScore == null ? 0 : compare(score, mBestScore);
					/*
					if(compare > 0){
						//mSamples.add(s);
						propagateRewards(mSamples, 1.0);
						lastScoreIteration = iteration;
						//mSamples.clear();
					}else if(iteration - lastScoreIteration > 0){
						int local_compare = lastScore == null ? 0 : compare(score,lastScore);
						if(local_compare > 0){
							//mSamples.add(s);
							if(score.getHardScore() > lastScore.getHardScore())
								propagateRewards(mSamples,0.5);
							else
								propagateRewards(mSamples,0.3);
							//mSamples.clear();
						}else if(local_compare < 0){
							//mSamples.add(s);
							//propagateRewards(mSamples, -0.3);
							//mSamples.clear();
						}
						lastScoreIteration = iteration;
						lastScore = score;
						calc.resetWorkingSolution(sch);
						
					}
					*/
					if(mBestScore == null || compare > 0)
					{
						System.out.println("New best local score " + score);
						mBestScore = score;
						//setBestSchedule(mBestScore, sch);
						
						//propagate rewards
						
						
					}
				}finally {
					mBestScoreSync.unlock();
				}
			}
			
			
			
			
			//iteration++;
		}while(mRunning);
		
		
	}
	
	
	private void action(int a, OperationIncrementalScoreCalculator calc, Operation selectedOperation, List<Operation> sorted, int opIndex ){
		if(a == ACTION_MOVE_FORWARD){
			doMove(calc, selectedOperation, (long)(selectedOperation.getStartDate() + 60 ));
		}else if(a == ACTION_STICK_NEXT){
			Operation next = opIndex < sorted.size() - 1 ? sorted.get(opIndex + 1) : null;
			if(next != null){
				doMove(calc, selectedOperation, next.getStartDate() - selectedOperation.getDuration());
			}
		}else if(a == ACTION_JUMP_NEXT){
			Operation next = opIndex < sorted.size() - 1 ? sorted.get(opIndex + 1) : null;
			if(next != null){
				doMove(calc, selectedOperation, next.getEndDate());
			}
		}else if(a == ACTION_MOVE_BACKWARDS){
			doMove(calc, selectedOperation, (long)(selectedOperation.getStartDate() - 60 ));
		}else if(a == ACTION_STICK_PREV){
			Operation prev = opIndex > 0 ? sorted.get(opIndex - 1) : null;
			if(prev != null){
				doMove(calc, selectedOperation, prev.getEndDate());
			}
		}else if(a == ACTION_JUMP_PREV){
			Operation prev = opIndex > 0 ? sorted.get(opIndex - 1) : null;
			if(prev != null){
				doMove(calc, selectedOperation, prev.getStartDate() - selectedOperation.getDuration());
			}
		}
	}
	
	private double[] features(Operation selectedOperation, SortedArrayList<Operation> sorted, int opIndex, List<Operation> ops){
		double[] features = new double[FEATURES_SIZE];			
		int feature_idx = 0;
		Resource opResource = selectedOperation.getResource();
		
		features[feature_idx++] = selectedOperation.getNextOperation() == null ? 0.0 : 1.0;
		
		//operation location features
		features[feature_idx++] = selectedOperation.getDuration()/(24*3600.0);	//duration
		features[feature_idx++] = (selectedOperation.getEndDate()%(24*3600))/(24*3600.0); //end time 
		
		Calendar cal = Calendar.getInstance();
		
		cal.setTimeInMillis(selectedOperation.getEndDate()*1000);
		int idx = cal.get(Calendar.DAY_OF_WEEK);	//note that Calendar.SUNDAY = 1
		features[feature_idx++] = (idx == Calendar.SUNDAY ? 6 : idx - 2)/10.0;	//day of week
		features[feature_idx++] = idx == Calendar.SUNDAY || idx == Calendar.SATURDAY ? 1 : 0;	//day of week
		
		//resource load features
		long day = selectedOperation.getStartDate()/(24*3600);
		long load = 0;
		for(Operation o : opResource.getOperationList()){
			if(o.getStartDate()/(24*3600) == day)
				load += o.getDuration();
		}
		features[feature_idx++] = load/(24*3600.0); //load over a 24H period
		features[feature_idx++] = selectedOperation.getDuration()/(1.0*load); //percentage of load occupied
		
		day -= 1;
		load = 0;
		for(Operation o : opResource.getOperationList()){
			if(o.getStartDate()/(24*3600) == day)
				load += o.getDuration();
		}
		features[feature_idx++] = load/(24*3600.0); //load over a 24H period
		day += 2;
		load = 0;
		for(Operation o : opResource.getOperationList()){
			if(o.getStartDate()/(24*3600) == day)
				load += o.getDuration();
		}
		features[feature_idx++] = load/(24*3600.0); //load over a 24H period
		
		
		
		long distanceToNext = 0;
		if(selectedOperation.getNextOperation() != null)
			distanceToNext = selectedOperation.getNextOperation().getStartDate() - selectedOperation.getEndDate();
		
		features[feature_idx++] = distanceToNext / (24*3600.0); //normalized so we get a smaller number
		
		distanceToNext = selectedOperation.getOverallEndDate() - selectedOperation.getEndDate();
		features[feature_idx++] = distanceToNext / (24*3600.0); //normalized so we get a smaller number
		features[feature_idx++] = distanceToNext / (1.0*selectedOperation.getOverallDuration());
		features[feature_idx++] = selectedOperation.getExtqty();
		features[feature_idx++] = selectedOperation.getUnitDuration();
		
		
		features[feature_idx++] = opIndex / (1.0*sorted.size()); //position within operations
		long distanceToPrev = (long)1e20;
		distanceToNext = (long)1e20;
		if(opIndex > 0)
			distanceToPrev = selectedOperation.getStartDate() - sorted.get(opIndex - 1).getEndDate();
		if(opIndex < sorted.size() - 1)
			distanceToNext = sorted.get(opIndex + 1).getStartDate() - selectedOperation.getEndDate();
		
		features[feature_idx++] = distanceToPrev / (24*3600.0);
		features[feature_idx++] = distanceToNext / (24*3600.0);
		
		distanceToPrev = (long)0;
		distanceToNext = (long)0;
		
		WorkOrder wo = selectedOperation.getWorkOrder();
		if(wo.getExecutionGroupForcedEnd() != null){
			distanceToNext = wo.getExecutionGroupForcedEnd() - selectedOperation.getEndDate();
		}
		if(wo.getExecutionGroupForcedStart() != null){
			distanceToPrev = selectedOperation.getStartDate() - wo.getExecutionGroupForcedStart();
		}
		
		
		features[feature_idx++] = distanceToPrev / (24*3600.0);
		features[feature_idx++] = distanceToNext / (24*3600.0);
		
		distanceToPrev = (long)1e20;
		for(Operation p : selectedOperation.getPreviousOperations()){
			long d = selectedOperation.getStartDate() - p.getEndDate();
			if(d < distanceToPrev)
				distanceToPrev = d;
		}
		features[feature_idx++] = distanceToPrev / (24*3600.0);
		
		
		//now rankings 20 ops behind and 20 ops ahead
		//and gaps between them
		double norm =  operationGroupRank(ops, selectedOperation, selectedOperation);
		if(norm == 0) 
			norm = 1.0;
		Operation pr = null;
		for(int i = -20 ; i <= 20; i++){
			int t = i + opIndex;
			Operation other = t >= 0 && t < sorted.size() - 1 ? sorted.get(t) : null;
			long rank = other == null ? 0 : operationGroupRank(ops, selectedOperation, other);
			features[feature_idx++] = rank / norm;
			long dly = 0;
			if(pr != null && other != null){
				dly = other.getStartDate() - pr.getEndDate();
			}
			features[feature_idx++] = dly/(24*3600.0);
			pr = other;
			
		}
		
		//spacing between ops
		for(int i = -20 ; i <= 20; i++){
			int t = i + opIndex;
			Operation other = t >= 0 && t < sorted.size() - 1 ? sorted.get(t) : null;
			distanceToNext = 0;
			if(other != null){
				Operation next = t >= 0 && t < sorted.size() - 2 ? sorted.get(t+1) : null;
				if(next != null){
					distanceToNext = next.getStartDate() - other.getStartDate();
				}
			}
			features[feature_idx++] = distanceToNext/(24*3600.0);	
		}
		
		Operation n = selectedOperation.getNextOperation();
		if(n != null){
			List<Operation> so = (List<Operation>) sorted.clone();
			so.addAll(n.getResource().getOperationList());
			int nidx = so.indexOf(n);
			distanceToPrev = (long)1e20;
			distanceToNext = (long)1e20;
			if(nidx > 0)
				distanceToPrev = n.getStartDate() - so.get(nidx - 1).getEndDate();
			if(nidx < so.size() - 1)
				distanceToNext = so.get(nidx + 1).getStartDate() - n.getEndDate();
			
			features[feature_idx++] = distanceToPrev / (24*3600.0);
			features[feature_idx++] = distanceToNext / (24*3600.0);
			
		}else{
			features[feature_idx++] = 0;
			features[feature_idx++] = 0;
		}
		
		
		return features;
		
	}
	
	
	private int compare(Score sc1, Score sc2){
		//long c1 = sc1.getHardScore() + sc1.getSoftScore();
		//long c2 = sc2.getHardScore() + sc2.getSoftScore();
		//return (int) (c1 - c2);
		return sc1.compareTo(sc2, true, true);
	}

	private void forward(double[] input, double[] output) {
		//mNeuroph.setInput(input);
		//mNeuroph.calculate();
		//double[] o = mNeuroph.getOutput();
		//System.arraycopy(o, 0, output, 0, o.length);
		
		//mANN.compute(input, output);
		float[] inp = new float[input.length];
		for(int i = 0; i < inp.length; i++)
			inp[i] = (float)input[i];
		float[] r = mFann.run(inp);
		for(int i = 0; i < r.length; i++)
			output[i] = r[i];
		
		
	}
	
	public static class ExFann extends Fann{

		
		static{
			NativeLibrary fann;
			if (Platform.isWindows()) {
				fann = NativeLibrary.getInstance("fannfloat");
				Map options = fann.getOptions();
				options.put("calling-convention", Integer.valueOf(1));
				options.put("function-mapper", new WindowsFunctionMapper());
			} else {
				fann = NativeLibrary.getInstance("fann");
			}
			Native.register(fann);
		}		
		
		public ExFann(List<Layer> layers) {
			super(layers);
		}

		public ExFann(String absolutePath) {
			super(absolutePath);
		}

		public void train(double[] inputs, double[] outputs){
			float[] finp = new float[inputs.length];
			float[] fout = new float[outputs.length];
			for(int i = 0; i < finp.length; i++){
				finp[i] = (float)inputs[i];
			}
			for(int i = 0; i < fout.length; i++){
				fout[i] = (float)outputs[i];
			}
			fann_train(ann, finp, fout);
			
		}
		
		public void setLearningRate(double lr) { fann_set_learning_rate(ann, (float)lr);}
		public void setMomentum(double m) { fann_set_learning_momentum(ann, (float)m);}
		
		protected static native void fann_train(Pointer paramPointer, float[] input, float[] output);
		protected static native void fann_set_learning_rate(Pointer paramPointer, float lr);
		protected static native void fann_set_learning_momentum(Pointer paramPointer, float m);
		
	}
	
	//greedy version
	private void train(List<Sample> samples){
		samples = new ArrayList<>(samples);
		int n = 10;
		while(n-- > 0){
			for(int i = samples.size() - 1; i >= 0; i--){
				Sample s = samples.get(i);
				
				double[] outs = new double[OUTPUT_SIZE];
				for(int o = 0; o < OUTPUT_SIZE; o++){
					outs[o] = -1.0;
				}
				outs[s.action] = 1.0;
				
				
				mFann.train(s.state, outs);
				/*
				System.out.println("----");
				println(outs);
				forward(s.state, outs); //dbg
				println(outs);
				*/
			}
		}
		mFann.save(networkFile.getAbsolutePath());
		
		
	}
	//fann version
	private void train__(List<Sample> samples){
		samples = new ArrayList<>(samples);
		
		
		for(int i = samples.size() - 1; i >= 0; i--){
			if(mRandom.nextBoolean())
				continue;
			
			
			
			//Sample s = samples.remove(mRandom.nextInt(samples.size()));
			Sample s = samples.get(i);
			double[] outs = new double[OUTPUT_SIZE];
			forward(s.newState, outs);
			double max_q = Double.MIN_VALUE;
			for(int o = 0; o < OUTPUT_SIZE; o++){
				if(outs[o] > max_q){
					max_q = outs[o];
				}
			}
			forward(s.state, outs);
			double lr = 0.1;
			//outs[s.action] = (1-lr)*outs[s.action] + lr*( s.reward + 0.3*max_q);
			outs[s.action] = outs[s.action] +0.1*( s.reward + 0.8*max_q -  outs[s.action]);
			//outs = new double[OUTPUT_SIZE];
			//outs[s.action] = s.reward + 0.3*max_q;
			
			
			mFann.train(s.state, outs);
			/*
			System.out.println("----");
			println(outs);
			forward(s.state, outs); //dbg
			println(outs);
			*/
		}
		
		
		//mTrainer = new Backpropagation(mANN, set, 0.3, 0.01);
		
		/*
		mTrainer.setErrorFunction(new ErrorFunction(){


			@Override
			public void calculateError(ActivationFunction af, double[] b, double[] a, double[] ideal, double[] actual,
					double[] error, double derivShift, double significance) {
				for(int i=0;i<actual.length;i++) {
					if(Math.abs(ideal[i]) < 1e-30){
						error[i] = 0;
					}else{
						double deriv = af.derivativeFunction(b[i],a[i]);// + derivShift;
						error[i] = ((ideal[i] - actual[i]) *significance) * deriv;						
					}
				}
				
			}
		});
		*/
		//mTrainer.iteration(5);
		//EncogDirectoryPersistence.saveObject(networkFile, (BasicNetwork)mANN);
		
		mFann.save(networkFile.getAbsolutePath());
		
		
	}
	
	private void train_(List<Sample> samples){
		samples = new ArrayList<>(samples);
		
		
		if(mTrainer == null){
			MLDataSet set = new BasicMLDataSet();
			set.add(new BasicMLDataPair(new BasicMLData(FEATURES_SIZE), new BasicMLData(OUTPUT_SIZE)));
			mTrainer = new StochasticGradientDescent(mANN, set);
			mTrainer.setLearningRate(0.6);
			mTrainer.setMomentum(0.3);
			mTrainer.setUpdateRule(new MomentumUpdate());
		}
		
		for(int i = samples.size() - 1; i >= 0; i--){
			if(mRandom.nextBoolean())
				continue;
			
			
			
			//Sample s = samples.remove(mRandom.nextInt(samples.size()));
			Sample s = samples.get(i);
			double[] outs = new double[OUTPUT_SIZE];
			mANN.compute(s.newState, outs);
			double max_q = Double.MIN_VALUE;
			for(int o = 0; o < OUTPUT_SIZE; o++){
				if(outs[o] > max_q){
					max_q = outs[o];
				}
			}
			mANN.compute(s.state, outs);
			double lr = 0.1;
			//outs[s.action] = (1-lr)*outs[s.action] + lr*( s.reward + 0.3*max_q);
			//outs[s.action] = outs[s.action] +0.04*( s.reward + 0.9*max_q -  outs[s.action]);
			//outs = new double[OUTPUT_SIZE];
			outs[s.action] = s.reward + 0.3*max_q;
			
			MLData inp = new BasicMLData(s.state);
			MLData ideal = new BasicMLData(outs);
			MLDataPair pair = new BasicMLDataPair(inp, ideal);
			//set.add(pair);
			
			
			
			
			for(int n = 0; n < 1000; n++){
				// Update the gradients based on this pair.
				mTrainer.process(pair);
	            // Update the weights, based on the gradients
				mTrainer.update();
			}
			
			System.out.println("----");
			println(outs);
			mANN.compute(s.state, outs); //dbg
			println(outs);
			
		}
		
		
		//mTrainer = new Backpropagation(mANN, set, 0.3, 0.01);
		
		/*
		mTrainer.setErrorFunction(new ErrorFunction(){


			@Override
			public void calculateError(ActivationFunction af, double[] b, double[] a, double[] ideal, double[] actual,
					double[] error, double derivShift, double significance) {
				for(int i=0;i<actual.length;i++) {
					if(Math.abs(ideal[i]) < 1e-30){
						error[i] = 0;
					}else{
						double deriv = af.derivativeFunction(b[i],a[i]);// + derivShift;
						error[i] = ((ideal[i] - actual[i]) *significance) * deriv;						
					}
				}
				
			}
		});
		*/
		//mTrainer.iteration(5);
		EncogDirectoryPersistence.saveObject(networkFile, (BasicNetwork)mANN);
		
		
		
		
	}

	private TrainingContinuation mContinuation = null;
	
	private void propagateRewards(List<Sample> samples, double reward) {
		propagateRewards(samples, reward, samples.size());
	}
	
	private void propagateRewards(List<Sample> samples, double reward, int l) {
		MLDataSet set = new SignificanceDataSet();
		//DataSet trainingSet = new DataSet(FEATURES_SIZE, OUTPUT_SIZE);
		double decay = 0.90;
		int n = 0;
		if(l>samples.size())
			l = samples.size();
		for(int i = samples.size() - 1 ; i >= samples.size() - l; i--){
			Sample s = samples.get(i);
			
			double discount = Math.pow(decay, n);
			n++;
			MLData inp = new BasicMLData(s.state);
			double[] wactions = new double[OUTPUT_SIZE];
			wactions[s.action] = 1.0;
			MLData ideal = new BasicMLData(wactions);			
			MLDataPair pair = new BasicMLDataPair(inp, ideal);
			pair.setSignificance(reward*discount);
			set.add(pair);
			
			//trainingSet.addRow(s.inputs, wactions);
			
			if(discount < 1e-5)
				break;
		}
		
		
//		BackPropagation bp =new BackPropagation();
//		bp.setErrorFunction(new org.neuroph.core.learning.error.ErrorFunction(){
//
//			private transient double totalError;
//		    
//		    /**
//		     * Number of patterns - n 
//		     */
//		    private transient double patternCount;
//
//		   
//
//		    
//		    @Override
//		    public double[] addPatternError(double[] predictedOutput, double[] targetOutput) {
//		        double[] patternError = new double[targetOutput.length];
//
//		        double r = 0;
//				for(int i = 0; i < targetOutput.length; i++){
//					if(Math.abs(targetOutput[i]) > Math.abs(r))
//						r = targetOutput[i];
//				}
//		        
//		        for (int i = 0; i < predictedOutput.length; i++) {
//		            patternError[i] =  predictedOutput[i] - targetOutput[i];
//		            totalError += patternError[i] * patternError[i];
//		            
//		            if( Math.abs(targetOutput[i]) > 0 )
//		            	patternError[i] = (predictedOutput[i] - 1) * targetOutput[i];
//					else
//						patternError[i] = predictedOutput[i]*r;
//
//					if(Double.isNaN(patternError[i]))
//						patternError[i] = 0;		            
//		            
//		        }
//		        
//		        patternCount++;
//		        return patternError;
//		        
//		    }
//		    
//		    @Override
//		    public void reset() {
//		        totalError = 0d;
//		        patternCount = 0;
//		    }
//
//		    @Override
//		    public double getTotalError() {
//		        return totalError / (2*patternCount );
//		    }
//
//			
//		});
//		bp.setMaxIterations(100);
//		mNeuroph.setLearningRule(bp);
//		mNeuroph.learn(trainingSet);
//		
//		System.out.println("Error after training "+bp.getTotalNetworkError());
//		//mNeuroph.save("planner.nnet");
		/*
		double[] inp = samples.get(0).inputs;
		double[] outs = new double[OUTPUT_SIZE];
		mANN.compute(inp, outs);
		System.out.println("Before training...");
		println(outs);
		*/
		//if(mTrainer == null)
		{
			//mTrainer = new Backpropagation(mANN, set, 0.3, 0.01);
			/*
			mTrainer.setErrorFunction(new ErrorFunction(){

				@Override
				public void calculateError(double[] ideal, double[] actual, double[] error) {
					//double loss = 0;
					
					double r = 0;
					for(int i = 0; i < ideal.length; i++){
						if(Math.abs(ideal[i]) > r)
							r = Math.abs(ideal[i]);
					}
					
					for(int i = 0; i < ideal.length; i++){
						//error[i] = ideal[i] - actual[i];
						
						//double loss = -1*ideal[i]*Math.log(actual[i] > 0 ? actual[i] : 1e-20);
						//error[i] = loss;
						//if( Math.abs(ideal[i]) > 0 )
						//	error[i] = (1 - actual[i]) * ideal[i];
						//else
						//	error[i] = -1*actual[i]*r;
						
						if(ideal[i] > 0)
							error[i] = (1 - actual[i])*r;
						else if(ideal[i] <= 0)
							error[i] = (0 - actual[i])*r;
						
						//error[i] = ideal[i];
						if(Double.isNaN(error[i]))
							error[i] = 0;
					}
					
					//for(int i = 0; i < actual.length; i++){
					//	error[i] = loss;
					//}
					
				}
				
			});
			*/
		}
		
		//mTrainer.setTraining(set);
		//if(mContinuation != null)
		//	mTrainer.resume(mContinuation);
		
		//EncogUtility.trainToError(mTrainer, 1e-4);
		
		mTrainer.iteration(3);
		System.out.println("Error after training "+mTrainer.getError());
		//mContinuation = mTrainer.pause();
		mTrainer.finishTraining();
		
		EncogDirectoryPersistence.saveObject(networkFile, (BasicNetwork)mANN);
		/*
		mANN.compute(inp, outs);
		System.out.println("After training...");
		println(outs);
		*/
		
	}
	
	
	private void println(double[] d){
		for(int i = 0; i < d.length; i++){
			System.out.print(d[i]);
			System.out.print("\t");
		}
		System.out.println();
		
	}

	private class Sample{
		public double[] state;
		public int action;
		public int reward;
		public double[] newState;
		
		
	}
	
	private class SignificanceDataSet extends BasicMLDataSet{
		
		
		public SignificanceDataSet(List<MLDataPair> data) {
			super(data);
		}


		public SignificanceDataSet() {
			super();
		}


		@Override
		public void getRecord(final long index, final MLDataPair pair) {

			super.getRecord(index, pair);			
			final MLDataPair source = this.get((int) index);
			pair.setSignificance(source.getSignificance());

		}
		
		
		@Override
		public MLDataSet openAdditional() {
			return new SignificanceDataSet(super.getData());
		}
		
	}
	
	
	
	
	
	/**
	 * move this and all next operations to their latest possible time
	 * @param sorted 
	 * @param o
	 */
	private int collapse_(OperationIncrementalScoreCalculator calc, SortedArrayList<Operation> sorted, Operation o, long maxStart) {
		int res = 0;
		Operation n = o.getNextOperation();
		if(n != null){
			res += collapse(calc, sorted, n, maxStart);
			long startTime = n.getStartDate() - o.getDuration();			
			if(startTime - o.getStartDate() > 2*o.getResource().getGroupingHorizon()){ //minimize impact on grouping
				
				if(acceptMove(o, startTime))
				{
					if(doMove(calc, o, startTime, false)){						
						res++;
						sorted.remove(o);
						sorted.add(o);
					}
				}
			}
		}else{
			long startTime = maxStart > 0 ? maxStart : o.getStartDate();	
			if(startTime != o.getStartDate()){
				if(acceptMove(o, startTime))
				{
					if(doMove(calc, o, startTime, false)){
						res++;
						sorted.remove(o);
						sorted.add(o);
					}
				}
			}
		}
		
		return res;
	}
	private int collapse(OperationIncrementalScoreCalculator calc, SortedArrayList<Operation> sorted, Operation o, long maxStart) {
		int res = 0;
		Operation n = o.getNextOperation();
		if(n != null){
			res += collapse(calc, sorted, n, maxStart);
			long startTime = n.getStartDate() - o.getDuration();
			boolean gapFound = false;
			
			Operation nxt = null;
			Operation prv = null;
			
			for(int i = sorted.size() -1 ; i >= 0; i--){
				prv = sorted.get(i);
				if(!prv.getResource().equals(o.getResource()))
					continue;
				if(nxt != null && nxt.getStartDate() >= 2*o.getResource().getGroupingHorizon()){ //minimize impact on grouping
				//if(prv.getEndDate() < startTime && nxt != null){
					long gap = nxt.getStartDate() - prv.getEndDate();
					if(gap >= o.getDuration()){
						long asap = asap(o, startTime, nxt, o.getResource());
						if(asap > prv.getEndDate()){
							startTime = asap;
							gapFound = true;
							break;
						}
					}
				}
				nxt = prv;
			}
			
			
			if(gapFound && startTime > o.getStartDate()){
				
				if(acceptMove(o, startTime))
				{
					if(doMove(calc, o, startTime, false)){						
						res++;
						sorted.remove(o);
						sorted.add(o);
					}
				}
			}
		}else{
			long startTime = maxStart > 0 ? maxStart : o.getStartDate();	
			if(startTime > o.getStartDate()){
				if(acceptMove(o, startTime))
				{
					if(doMove(calc, o, startTime, false)){
						res++;
						sorted.remove(o);
						sorted.add(o);
					}
				}
			}
		}
		
		return res;
	}

	protected long asap(Operation[] sorted, Resource res, int opIndex, long targetStart){
		return asap(sorted[opIndex], targetStart, opIndex < sorted.length - 1 ? sorted[opIndex+1] : null, res);
	}
	protected long asap(List<Operation> sorted, Resource res, int opIndex, long targetStart){
		return asap(sorted.get(opIndex), targetStart, opIndex < sorted.size() - 1 ? sorted.get(opIndex+1) : null, res);
	}
	protected long asap(Operation o, long targetStart, Operation next, Resource res){
		
		long maxEndTime = targetStart + o.getDuration();
		if(o.getNextOperation() != null && o.getNextOperation().getStartDate()  < maxEndTime ){
			maxEndTime = o.getNextOperation().getStartDate();
		}
		if(next != null){
			if(next.getStartDate() < maxEndTime){
				maxEndTime = next.getStartDate();
			}
		}
		
		long hour = (maxEndTime % (24*3600));
		long dayT = ((maxEndTime) /(24*3600))*24*3600;
		
		//assert(endHour < 24*3600);
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		if(hour > 18*3600){
			hour = (long)(18*3600);
		}else if(hour > 12.5*3600 && hour < 14*3600){
			hour = (long) (12.5*3600);
		}else if(hour <= 8.5*3600 || hour > 24*3600){
			hour = (long) (18*3600);
	        cal.setTimeInMillis(dayT*1000);
	        int idx = cal.get(Calendar.DAY_OF_WEEK);	//note that Calendar.SUNDAY = 1
	        if(idx == 2)
	        	dayT -= 3*24*3600;
	        else if(idx == 1)
	        	dayT -= 2*24*3600;
	        else
	        	dayT -= 24*3600;
		}
		
		maxEndTime = dayT + hour;
		
		return maxEndTime - o.getDuration();
		
		
	}
	
	
	private void log(String message){
		System.out.println(Thread.currentThread().getId()+" > " +  message);
	}
	
	
	protected boolean acceptMove(Operation o, Long target){
		if(1*target == o.getStartDate())
			return false;
		if(!o.isMovable())
			return false;
		
	
		/*
		//tireredess takes too long to converge with a lot of OFs
		Long t = mOperationTiredness.getOrDefault(o.getCode(), 0L);
		
		mOperationTirednessMax =  Math.max(mOperationTirednessMax, t);
		if(mOperationTirednessMax > 0 && (10.0*(t/(double)mOperationTirednessMax) < Math.abs(mRandom.nextGaussian())))
		//if((mRandom.nextDouble() * mOperationTirednessMax + 0.5) >= 0.5*t)
			return true;
		
		return false;
		*/
	
		List<Long> os = mLastMoves.get(o);
		if(os == null){
			os = new ArrayList<Long>(30);
			mLastMoves.put(o, os);
		}
//		
//		/*
//		if(os.size() > 0){
//			if(mRandom.nextDouble() < 0.90)
//				os.remove(0);
//		}
//		if(os.size() > 0)
//			return false;
//		os.add(target);
//		return true;
//		*/
//		
		if(os.size() > 0 && (mRandom.nextDouble() > 0.80 || os.size() > 20)){
		//while(os.size() > 30){
			os.remove((int)(mRandom.nextInt(os.size())));
		}
		boolean res = !os.contains(target);
		//by allowing duplicates we make sure the targets most used remain in the list the most time
		//thus disallowing it's usage
		if(res)
			os.add(target);
		return res;
//		//return true;
	}
	
	
	public void doMove(Operation o, long newStart){
		lockWorkingSolution();
		if(mCalculator != null){
			doMove(mCalculator, o,  newStart);
		}else{			
			o.setStartDate(newStart);
			mWorkingSolutionChanged = true;
		}
		releaseWorkingSolution(false);
	}
	
	protected boolean doMove(OperationIncrementalScoreCalculator calc, Operation o, long targetStart){
		return doMove(calc, o, targetStart, false);
	}
		
	protected boolean doMove(OperationIncrementalScoreCalculator calc, Operation o, long targetStart, boolean movePredecessors){	
		
		if(!o.isMovable())
			return false;
		if(targetStart == o.getStartDate())
			return false;
		
		Long t = mOperationTiredness.get(o.getCode());
		if(t == null || t == 0){
			t = mOperationTirednessMax > 1 ? mOperationTirednessMax/2 : 1;
			//t = 1L;
		}else if (t < 1e50){
			t += Math.max(1, mOperationTirednessMax / t);
			//t += 1;
		}
		mOperationTiredness.put(o.getCode(), t);
		//if(1*targetStart > o.getMaxStartDate())
		//	return;

		calc.beforeVariableChanged(o, "startDate");
		o.setStartDate(targetStart);
		calc.afterVariableChanged(o, "startDate");

		//if("0011711MFG00019486.5".equals(o.getCode()) || "0011711MFG00018207.5".equals(o.getCode())){
		//	log(o.toString()+" "+Instant.ofEpochSecond(o.getStartDate()).toString());
		//}
		
		WorkOrder wo = o.getWorkOrder();
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		for(Operation op : wo.getOperations()){
			if(op.getStartDate() < min)
				min = op.getStartDate();
			if(op.getEndDate() > max)
				max = op.getEndDate();
		}
		if(min < Long.MAX_VALUE || max > Long.MIN_VALUE){
			//as long as there is one operation, there will be both min and max
			if(min != 1*wo.getFirstStart() || max != 1*wo.getLastEnd()){					
				calc.beforeVariableChanged(wo, "firstStart");
				wo.setFirstStart(min);
				wo.setLastEnd(max);					
				calc.afterVariableChanged(wo, "firstStart");
			}
		}
		/*
		if(movePredecessors){
			for(Operation p : o.getPreviousOperations()){
				if(p.getEndDate() > o.getStartDate()){
					doMove(calc, p, o.getStartDate() - p.getDuration(), true);
				}
			}
		}
		*/
		if(movePredecessors){
			for(Operation p : o.getPreviousOperations()){
				if(p.getEndDate() > o.getStartDate()){
					doMove(calc, p, o.getStartDate() - p.getDuration(), true);
				}
			}
			
			if(o.getNextOperation() != null && o.getNextOperation().getStartDate() < o.getEndDate()){
				doMove(calc, o.getNextOperation(), o.getEndDate());
			}
		}
		
		return true;
		
	}
	
	
	public void doMove(Operation o, Resource newResource){
		lockWorkingSolution();
		if(mCalculator != null){
			doMove(mCalculator, o,  newResource);
		}else{
			o.getResource().getOperationList().remove(o);		
			o.setResource(newResource);
			newResource.getOperationList().add(o);
			mWorkingSolutionChanged = true;
		}
		releaseWorkingSolution(false);
	}

	
	protected boolean doMove(OperationIncrementalScoreCalculator calc, Operation o, Resource res){
		
		//if(!o.isMovable())
		//	return false;
		//if(res.equals(o.getResource()))
		//	return false;
		
		calc.beforeVariableChanged(o, "resource");
		
		o.getResource().getOperationList().remove(o);		
		o.setResource(res);
		assert(!res.getOperationList().contains(o));
		res.getOperationList().add(o);
		calc.afterVariableChanged(o, "resource");
		return true;
	}
	

	private class CacheKey {

	    private final String x;
	    private final String y;
	    

	    public CacheKey(String x, String y) {
	        this.x = x;
	        this.y = y;
	    }

	    @Override
	    public boolean equals(Object o) {
	        if (this == o) return true;
	        if (!(o instanceof CacheKey)) return false;
	        CacheKey key = (CacheKey) o;
	        return (Objects.equals(x, key.x) && Objects.equals(y, key.y)) || (Objects.equals(x, key.y) && Objects.equals(y, key.x));
	    }

	    @Override
	    public int hashCode() {
	    	if(x.compareTo(y) < 0)
	    		return new HashCodeBuilder().append(x).append(y).toHashCode();
	    	else
	    		return new HashCodeBuilder().append(y).append(x).toHashCode();
	    }

	}
	
	
	
	
	protected Map<CacheKey, Integer> computeGroupRankCache(List<Operation> ops, long startTime){
		
		Map<CacheKey, Integer> cache = new HashMap<CacheKey, Integer>();
		
		for(int i = 0; i < ops.size(); i++)
		{
			
			if(theLastRankComputeTime > startTime)
				return null;	//a new compute task is running;
			
			Operation o1 = ops.get(i);
			List<StockItem> consumedPrev = new ArrayList<StockItem>(10);
			List<StockItem> producedPrev = new ArrayList<StockItem>(10);
			List<String> groupsPrev = new ArrayList<String>();
			WorkOrder wo1 = o1.getWorkOrder();
			
			double resWeight = 1.0;
			if(o1.getResource() != null){
				if("LINPINT".equals(o1.getResource().getCode())){
					resWeight = 2.0;
				}
			}
			
			for(StockItemTransaction tr : wo1.getRequiredTransaction()){
				StockItem it = tr.getItem();
				consumedPrev.add(it);
			}
			for(StockItemTransaction tr : wo1.getProducedTransactionList()){
				producedPrev.add(tr.getItem());
			}
			
			for(ExecutionGroup g : wo1.getExecutionGroups()){
				groupsPrev.add(g.getCode());
				
			}
			
			int bestScore = 0;
			for(int j = i + 1; j < ops.size(); j++)
			{
				
				if(theLastRankComputeTime > startTime)
					return null;	//a new compute task is running;
				
				Operation o2 = ops.get(j);
			
				if(!o1.getResourceRange().contains(o2.getResource()))
					continue;
				
				WorkOrder wo2 = o2.getWorkOrder();
				int consumedMaterialsScore = 0;
				int producedItemsScore = 0;
				int descriptionMatchScore = 0;
				int groupMatchScore = 0;
				
				for(StockItemTransaction tr : wo2.getRequiredTransaction()){
					StockItem it = tr.getItem();
					if(consumedPrev.contains(it)){
						consumedMaterialsScore += it.getReocod() != 3 ? 100*resWeight : 2;
					}
				}
				
				for(ExecutionGroup g : wo2.getExecutionGroups()){
					if(groupsPrev.contains(g.getCode())){
						groupMatchScore++;
					}
					
				}
				
				
				int minLevenshteinDistance = Integer.MAX_VALUE;
				int maxStrlen = 0;
				for(StockItemTransaction tr : wo2.getProducedTransactionList()){
					StockItem it = tr.getItem();
					if(producedPrev.contains(it)){
						producedItemsScore++;
					}
					String desc = it.getDescription();
					if(desc != null){
						int l = desc.length();
						if(l > 0){
							if(l > maxStrlen)
								maxStrlen = l;
							for(StockItem p : producedPrev){
								int d = StringUtils.getLevenshteinDistance(it.getDescription(), p.getDescription(), 2*l/3);
								if(d >=0 && d < minLevenshteinDistance){
									minLevenshteinDistance = d;
								}
							}
							
						}
					}
				}
				
				
				if(maxStrlen > 0 && minLevenshteinDistance < Integer.MAX_VALUE){
					double r = 1 - (minLevenshteinDistance / (double)maxStrlen);
					r *= 10;
					descriptionMatchScore += r;
				}
				
				int val = descriptionMatchScore + 100*groupMatchScore + 1000*producedItemsScore + 100000*consumedMaterialsScore;
				
				
				if(val > bestScore)
					bestScore = val;
				
				if(val > 0)
					cache.put(new CacheKey(o1.getCode(), o2.getCode()), val);
				
				//operationGroups[i][j] = (byte)(val > 127 ? 127 : val);
				//operationGroups[j][i] = (byte)(val > 127 ? 127 : val);
			}
			consumedPrev.clear();
			consumedPrev = null;
			producedPrev.clear();
			producedPrev = null;
			
			if(bestScore > 0)
				cache.put(new CacheKey(o1.getCode(), o1.getCode()), bestScore);
			//operationGroups[i][i] = (byte)(bestScore > 127 ? 127 : bestScore);
		}
		return cache;
	}
	
	
	protected int operationGroupRank(List<Operation> ops, int a, int b){
		return operationGroupRank(ops, ops.get(a), ops.get(b));
	}
	
	protected int operationGroupRank(List<Operation> ops, Operation a, Operation b){
		
		int v = 0;
		synchronized (groupRankCacheSync) {
			if(groupRankCache != null){
				CacheKey k = new CacheKey(a.getCode(), b.getCode());
				v = groupRankCache.getOrDefault(k, 0);
			}
		}
		
		return v;

	}
	
	
	
	
	private void updateNextOperations(List<Operation> ops , List<WorkOrder> allWo, Map<String, List<WorkOrder>> invoiceMap) {
		
		for(Operation o : ops){
			o.setNextOperation(null);
			o.getPreviousOperations().clear();
		}
		
		
		//replicate the sequence as imposed by MRP for each vcr
		//first for all operations in a workorder enforce sequence
		List<Operation> terminalOps = new ArrayList<>(allWo.size());
		List<Operation> starterOps = new ArrayList<>(ops);
		for(WorkOrder w : allWo){
			List<Operation> wops = w.getOperations();
			Map<Integer, Operation> openums = new HashMap<>();
			for(Operation o : wops){
				openums.put(o.getOPENUM(), o);
			}
			for(Operation o : wops){
				int next = o.getNEXOPENUM();
				Operation n = openums.get(next);
				o.setNextOperation(n); //if null so be it
				if(n == null)
					terminalOps.add(o);
				else
					starterOps.remove(n);
			}
		}
		List<StockItemTransaction> usedTrans = new ArrayList<>(allWo.size());
		//repeat untill no more matching can be done
		//even if it means putting more than one op pointing to the same transaction
		int iterations = 0;
		do{
			iterations ++;
			usedTrans.clear();
			for(int i = terminalOps.size() - 1; i >= 0; i--){
				Operation o = terminalOps.get(i);
				WorkOrder wo = o.getWorkOrder();
				StockItemProductionTransaction stockProduction = wo.getProducedTransactionList().get(0);
				List<WorkOrder> consumers = new LinkedList<>();
				List<StockItemTransaction> consumerTransaction = new LinkedList<>();
				for(WorkOrder c : invoiceMap.get(stockProduction.getVCR())){
					List<StockItemTransaction> reqs = c.getRequiredTransaction();
					for(StockItemTransaction r : reqs){
						if(usedTrans.contains(r))
							continue;
	
						if(r.getItem().equals(stockProduction.getItem()) && !consumers.contains(c)){
							consumers.add(c);
							consumerTransaction.add(r);
						}
					}
				}
				WorkOrder consumer = null;
				StockItemTransaction consumerTrans = null;
				for(int j = 0; j < consumers.size(); j++){
					WorkOrder c = consumers.get(j);
					StockItemTransaction r = consumerTransaction.get(j);
					if(Math.abs(r.getQuantity() - stockProduction.getQuantity()) < 0.0001){
						//we have a qty match, override any previous result and use this instead
						consumer = c; consumerTrans = r;
						break;
					}else if(consumer == null){
						consumer = c; consumerTrans = r;	//hang on the first one found
					}
				}
				
				if(consumer != null){
					usedTrans.add(consumerTrans);
					for(Operation n : consumer.getOperations()){
						if(starterOps.contains(n)){
							o.setNextOperation(n);
							terminalOps.remove(i);
							break;
						}
					}
				}
				
			}
		}while(usedTrans.size() > 0 && iterations < 100);
		
		
		//any workorder with more than one operation on the same openum
		//was split and needs to be sequenced
		for(WorkOrder wo : allWo){
			
			Map<Integer, List<Operation>> openumOps = new HashMap<>(wo.getOperations().size());
			for(Operation o : wo.getOperations()){
				List<Operation> wops = openumOps.get(o.getOPENUM());
				if(wops == null){
					wops = new LinkedList<Operation>();
					openumOps.put(o.getOPENUM(), wops);
				}
				wops.add(o);				
			}
			
			for(List<Operation> wops : openumOps.values()){
				if(wops.size() <= 1)
					continue;
				
				Operation next = null;
				for(Operation o : wops){
					if(o.getNextOperation() != null){
						next = o.getNextOperation();
						break;
					}
				}
				for(int i = wops.size() - 1 ; i >= 0; i--){
					Operation o = wops.get(i);
					o.setNextOperation(null);
				}
				assert(next != null);
				for(int i = wops.size() - 1 ; i >= 0; i--){
					Operation o = wops.get(i);
					o.setNextOperation(next);
					next = o;					
				}
				
			}
			
			
			
			
		}
		
		//commercial BOMs generate false positives
		for(Operation o : ops){
			if(!"EXPE".equals(o.getResource().getCode())){
				if(o.getNextOperation() == null){
					log("Invalid next operation for "+o.toString());
				}
			}
		}
		
		
		
	}


	public void terminateEarly() {
		mRunning = false;
	}
	
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
