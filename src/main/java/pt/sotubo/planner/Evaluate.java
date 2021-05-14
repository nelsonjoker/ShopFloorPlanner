package pt.sotubo.planner;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

import javax.imageio.ImageIO;

import org.apache.commons.lang3.SerializationUtils;

import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Resource;
import pt.sotubo.planner.domain.Schedule;
import pt.sotubo.planner.domain.StockItem;
import pt.sotubo.planner.domain.StockItemProductionTransaction;
import pt.sotubo.planner.domain.StockItemTransaction;
import pt.sotubo.planner.domain.WorkOrder;
import pt.sotubo.planner.solver.Score;
import pt.sotubo.planner.solver.StockItemCapacityTracker;

public class Evaluate {

	public static void main(String[] args) throws FileNotFoundException {
		Schedule mSchedule = null;
		try {
			mSchedule = (Schedule) SerializationUtils.deserialize(new FileInputStream("solution_dbg.data"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		//generateBottleTest(mSchedule);
		
		Evaluate evaluater = new Evaluate();
		//evaluater.generateMosekTest(mSchedule);
		//evaluater.generateOrToolsTest(mSchedule);
		evaluater.evaluateResourceHTML(mSchedule, "LINPINT");
		//evaluater.evaluateLoadPerDay(mSchedule);
		//evaluater.evaluateStockRupture(mSchedule);
		
		evaluater.evaluateVCRNUMS_HTML(mSchedule);
		
	}
	
	private static void generateBottleTest(Schedule sch) throws FileNotFoundException {
		
		//List<WorkOrder> wos = sch.getWorkOrderList();
		List<Resource> resources = sch.getResourceList();
		List<Operation> operations = sch.getOperationList();
		
		Map<String, List<Operation>> vcrOps = new HashMap<>();
		for(Operation o : operations){
			WorkOrder wo = o.getWorkOrder();
			String vcr = wo.getProducedTransactionList().get(0).getVCR();
			List<Operation> ops = vcrOps.get(vcr);
			if(ops == null){
				ops = new ArrayList<>();
				vcrOps.put(vcr, ops);
			}
			ops.add(o);			
		}
		
		PrintStream of = new PrintStream("./bottle.prb");
		of.println("test setup");
		int jobs = vcrOps.size();
		int machines = resources.size();
		of.printf("%d  %d", jobs, machines);
		of.println();
		
		
		for(String vcr : vcrOps.keySet()){
			List<Operation> ops = vcrOps.get(vcr);
			
			LinkedList<Operation> seq = new LinkedList<>();
			
			while(ops.size() > 0){
				for(int i = ops.size() - 1; i >= 0 ; i--){
					Operation o = ops.get(i);
					if(o.getNextOperation() == null){
						seq.addLast(o);
						ops.remove(i);
						continue;
					}
					
					int nxtIdx = seq.indexOf(o.getNextOperation());
			    	if(nxtIdx >= 0){
			    		seq.add(nxtIdx, o);
			    		ops.remove(i);
			    	}
				}
			}
			ops.addAll(seq);
		}
		
		int maxLength = 0;
		for(String vcr : vcrOps.keySet()){
			List<Operation> ops = vcrOps.get(vcr);
			if(ops.size() > maxLength){
				maxLength = ops.size();
			}
		}
		
		for(String vcr : vcrOps.keySet()){
			List<Operation> ops = vcrOps.get(vcr);
			
			for(int i = 0; i < ops.size(); i++){
				Operation o = ops.get(i);
				int resIdx = resources.indexOf(o.getResource());
				int duration = o.getDuration();
				of.printf(" %d %d ", resIdx, duration);
			}
			int padding = maxLength - ops.size();
			while(padding-->0){
				of.printf(" %d %d ", 0, 0);
			}
			of.println();
			
		}
		
		
		of.close();
	}

	

	private void generateMosekTest(Schedule sch) throws FileNotFoundException {
		
		long tstart = 0;
		long tend = 30*24*3600 / 60;
		long M = 30*24*3600;
		
		PrintStream of = new PrintStream("./mosek.lp");
		of.println("minimize");
		of.println("\tobj: cmax");
		of.println();
		of.println("subject to");
		of.println();
		int constraintCount = 0;
		
		List<WorkOrder> wos = sch.getWorkOrderList();
		List<Resource> resources = sch.getResourceList();
		List<Operation> operations = sch.getOperationList();
		
		Map<Resource, Integer> nPossibleResourceOps = new HashMap<>();
		
		//while(wos.size() > 10)
		//	wos.remove(0);
		
		
		Map<StockItem, WorkOrder> itmWos = new HashMap<>();
		for(WorkOrder wo : wos){
			StockItem itm = wo.getProducedTransactionList().get(0).getItem();
			WorkOrder iwo = itmWos.get(itm);
			if(iwo == null){
				itmWos.put(itm, wo);
			}else{
				for(int i = 0; i < wo.getProducedTransactionList().size(); i++){
					StockItemTransaction tr = wo.getProducedTransactionList().get(i);
					StockItemTransaction itr = iwo.getProducedTransactionList().get(i);
					itr.setQuantity(itr.getQuantity() + tr.getQuantity());
					
				}
				for(int i = 0; i < wo.getRequiredTransaction().size(); i++){
					StockItemTransaction tr = wo.getRequiredTransaction().get(i);
					StockItemTransaction itr = iwo.getRequiredTransaction().get(i);
					itr.setQuantity(itr.getQuantity() + tr.getQuantity());
				}
				for(int i = 0; i < wo.getOperations().size(); i++){
					Operation op = wo.getOperations().get(i);
					if(i < iwo.getOperations().size()){
						Operation iop = iwo.getOperations().get(i);
						iop.setDuration(iop.getDuration() + op.getDuration());
					}
				}
				
			}
			
		}
		wos = new ArrayList<>( itmWos.values());
		itmWos.clear();
		itmWos = null;
		
		
		
		
		for(int i = 0; i < wos.size(); i++){
			wos.get(i).setMFGNUM("W"+i);
		}
		for(int i = 0; i < resources.size(); i++){
			resources.get(i).setCode("R"+i);
		}
		for(int i = 0; i < operations.size(); i++){
			operations.get(i).setCode("O"+i);
		}
		
		for(Resource r : resources){
			int p = 0;
			for(Operation o : operations){
				if(o.getResourceRange().contains(r))
					p++;
			}
			nPossibleResourceOps.put(r, p);
		}
		
		StringBuilder sb = new StringBuilder();
		
		//2.1, 2.2
		for(int i = 0; i < wos.size(); i++){
			
			WorkOrder w = wos.get(i);
			List<Operation> wops = w.getOperations();
			String ci = "ci_"+w.getMFGNUM();
			of.println("C"+(constraintCount++)+": cmax - "+ci+" >= 0");
			sb.setLength(0);
			
			Operation o = wops.get(wops.size()-1);
			List<Resource> res = o.getResourceRange();
			
			for(int k = 0; k < res.size(); k++){
				Resource r = res.get(k);
				String cijk = "cijk_"+w.getMFGNUM()+"_"+o.getCode()+"_"+r.getCode();
				sb.append(" + "); sb.append(cijk);
			}
			
			of.println("C"+(constraintCount++)+": "+sb.toString()+" - "+ci+" <= 0");
		}
		
		//2.3, 2.4
		for(int i = 0; i < wos.size(); i++){
			
			WorkOrder w = wos.get(i);
			List<Operation> wops = w.getOperations();
			String ci = w.getMFGNUM();

			for(int j = 0; j < wops.size(); j++){
				Operation o = wops.get(j);
				List<Resource> res = o.getResourceRange();
				
				for(int k = 0; k < res.size(); k++){
					Resource r = res.get(k);
					String sijk = "sijk_"+w.getMFGNUM()+"_"+o.getCode()+"_"+r.getCode();
					String cijk = "cijk_"+w.getMFGNUM()+"_"+o.getCode()+"_"+r.getCode();
					String vijk = "vijk_"+w.getMFGNUM()+"_"+o.getCode()+"_"+r.getCode();
					
					of.println("C"+(constraintCount++)+": "+sijk+" + "+cijk+" - "+M+" "+vijk+" <= 0");
					
					int pkij = o.getDuration();
					
					//of.println(sijk+" + "+pkij+" - (1 - "+vijk+") "+M+" - "+cijk+" <= 0");
					of.println("C"+(constraintCount++)+": "+sijk+" + "+M+" "+vijk+" - "+cijk+" <= "+(M - pkij));
				}
			}
		}
		
		//2.5, 2.6
		for(int h = 0; h < wos.size(); h++){
			WorkOrder wh = wos.get(h);
			for(int i = 0; i < h; i++){
				WorkOrder wi = wos.get(i);
				for(int j = 0; j < wi.getOperations().size(); j++){
					Operation oj = wi.getOperations().get(j);
					for(int g = 0; g < wh.getOperations().size(); g++){
						Operation og = wh.getOperations().get(g);
						List<Resource> Mij = new ArrayList<>(og.getResourceRange());
						Mij.retainAll(oj.getResourceRange());
						for(int k = 0; k < Mij.size(); k++){
							Resource r = Mij.get(k);
							String sijk = "sijk_"+wi.getMFGNUM()+"_"+oj.getCode()+"_"+r.getCode();
							String chgk = "cijk_"+wh.getMFGNUM()+"_"+og.getCode()+"_"+r.getCode();
							String zijhgk = "zijhgk_"+wi.getMFGNUM()+"_"+oj.getCode()+"_"+wh.getMFGNUM()+"_"+og.getCode()+"_"+r.getCode();
							
							String shgk = "sijk_"+wh.getMFGNUM()+"_"+og.getCode()+"_"+r.getCode();
							String cijk = "cijk_"+wi.getMFGNUM()+"_"+oj.getCode()+"_"+r.getCode();
							
							of.println("C"+(constraintCount++)+": "+chgk+" - "+M+" "+zijhgk+" - "+sijk+" <= 0");
							//of.println(cijk+" - ( 1 - "+zijhgk+" ) "+M+" - "+shgk+" <= 0");
							of.println("C"+(constraintCount++)+": "+cijk+" + "+M+" "+zijhgk+" - "+shgk+" <= "+M);
						}
					}
				}
			}
		}
		
		//2.7
		for(int i = 0; i < wos.size(); i++){
			WorkOrder wi = wos.get(i);
			for(int j = 1; j < wi.getOperations().size(); j++){
				sb.setLength(0);
				Operation oj = wi.getOperations().get(j);
				for(int k = 0; k < oj.getResourceRange().size(); k++){
					Resource r = oj.getResourceRange().get(k);
					String sijk = "sijk_"+wi.getMFGNUM()+"_"+oj.getCode()+"_"+r.getCode();
					sb.append(" + ");
					sb.append(sijk);
				}
				Operation oj1 = wi.getOperations().get(j-1);
				for(int k = 0; k < oj1.getResourceRange().size(); k++){
					Resource r = oj1.getResourceRange().get(k);
					String sijk = "sijk_"+wi.getMFGNUM()+"_"+oj1.getCode()+"_"+r.getCode();
					sb.append(" - ");
					sb.append(sijk);
				}
				sb.append(" >= 0");
				of.println("C"+(constraintCount++)+": "+sb.toString());
			}
		}
		
		
		//2.8
		for(int i = 0; i < wos.size(); i++){
			WorkOrder wi = wos.get(i);
			for(int j = 0; j < wi.getOperations().size(); j++){
				sb.setLength(0);
				Operation oj = wi.getOperations().get(j);
				for(int k = 0; k < oj.getResourceRange().size(); k++){
					Resource r = oj.getResourceRange().get(k);
					String vijk = "vijk_"+wi.getMFGNUM()+"_"+oj.getCode()+"_"+r.getCode();
					sb.append(" + ");
					sb.append(vijk);
				}
				sb.append(" = 1");
				of.println("C"+(constraintCount++)+": "+sb.toString());
			}
		}
		
		of.println();
		of.println("bounds");
		of.println();
		for(int i = 0; i < wos.size(); i++){
			WorkOrder wi = wos.get(i);
			String ci = "ci_"+wi.getMFGNUM();
			of.println(ci+" >= 0");
			for(int j = 0; j < wi.getOperations().size(); j++){
				Operation oj = wi.getOperations().get(j);
				for(int k = 0; k < oj.getResourceRange().size(); k++){
					Resource r = oj.getResourceRange().get(k);
					String sijk = "sijk_"+wi.getMFGNUM()+"_"+oj.getCode()+"_"+r.getCode();
					String cijk = "cijk_"+wi.getMFGNUM()+"_"+oj.getCode()+"_"+r.getCode();
					of.println(sijk+" >= 0");
					of.println(cijk+" >= 0");
				}
			}
		}
		
		of.println();
		of.println("general");
		of.println();
		of.println("cmax");
		for(int i = 0; i < wos.size(); i++){
			
			WorkOrder w = wos.get(i);
			List<Operation> wops = w.getOperations();
			String ci = "ci_"+w.getMFGNUM();
			of.println(ci);
			sb.setLength(0);
			
			Operation o = wops.get(wops.size()-1);
			List<Resource> res = o.getResourceRange();
			
			for(int k = 0; k < res.size(); k++){
				Resource r = res.get(k);
				String cijk = "cijk_"+w.getMFGNUM()+"_"+o.getCode()+"_"+r.getCode();
				String sijk = "sijk_"+w.getMFGNUM()+"_"+o.getCode()+"_"+r.getCode();
				of.println(cijk);
				of.println(sijk);
			}
		}
		
		
		of.println();
		of.println("binary");
		of.println();
		for(int i = 0; i < wos.size(); i++){
			WorkOrder wi = wos.get(i);
			for(int j = 0; j < wi.getOperations().size(); j++){
				Operation oj = wi.getOperations().get(j);
				for(int k = 0; k < oj.getResourceRange().size(); k++){
					Resource r = oj.getResourceRange().get(k);
					String vijk = "vijk_"+wi.getMFGNUM()+"_"+oj.getCode()+"_"+r.getCode();
					of.println(vijk);
				}
			}
		}
		for(int h = 0; h < wos.size(); h++){
			WorkOrder wh = wos.get(h);
			for(int i = 0; i < h; i++){
				WorkOrder wi = wos.get(i);
				for(int j = 0; j < wi.getOperations().size(); j++){
					Operation oj = wi.getOperations().get(j);
					for(int g = 0; g < wh.getOperations().size(); g++){
						Operation og = wh.getOperations().get(g);
						List<Resource> Mij = new ArrayList<>(og.getResourceRange());
						Mij.retainAll(oj.getResourceRange());
						for(int k = 0; k < Mij.size(); k++){
							Resource r = Mij.get(k);
							String zijhgk = "zijhgk_"+wi.getMFGNUM()+"_"+oj.getCode()+"_"+wh.getMFGNUM()+"_"+og.getCode()+"_"+r.getCode();
							of.println(zijhgk);
						}
					}
				}
			}
		}
		

		of.println();
		of.println("end");
		of.println();

	}
	/*
	private void generateOrToolsTest(Schedule sch) {
		
		List<WorkOrder> wos = sch.getWorkOrderList();
		Map<String, List<WorkOrder>> wosPerVCR = new HashMap<String, List<WorkOrder>>(); 
		
		for(WorkOrder wo : wos){
			String vcr = wo.getProducedTransactionList().get(0).getVCR();
			List<WorkOrder> list = wosPerVCR.get(vcr);
			if(list == null){
				list = new LinkedList<WorkOrder>();
				wosPerVCR.put(vcr, list);
			}
			list.add(wo);
		}
		
		List<Resource> resourceList = new ArrayList<>();
		Map<String, List<Operation>> vcrOps = new HashMap<String, List<Operation>>();
		for(String vcr : wosPerVCR.keySet()){
			List<WorkOrder> list = wosPerVCR.get(vcr);
			//when we find a work order that no one references as next this is our start point
			List<WorkOrder> copy = new LinkedList<>(list);
			for(WorkOrder wo : list){
				WorkOrder n = wo.getNextOrder();
				if(n != null){
					copy.remove(n);
				}
			}
			//assert(copy.size() == 1); //there can be only one
			List<Operation> ops = new LinkedList<Operation>();
			vcrOps.put(vcr, ops);
			
			WorkOrder w = copy.get(0);
			do{
				for(Operation o : w.getOperations()){
					ops.add(o);
					if(!resourceList.contains(o.getResource())){
						resourceList.add(o.getResource());
					}
				}
			}while((w = w.getNextOrder()) != null);
			
			
		}
		
		//on to the resources
		int wIndex = -1;
		for(String vcr : vcrOps.keySet()){
			wIndex++;
			List<Operation> ops = vcrOps.get(vcr);
			log("taskList = new List<Task>();");
			for(int oIndex = 0; oIndex < ops.size(); oIndex++){
				Operation o = ops.get(oIndex);
				int rIndex = resourceList.indexOf(o.getResource());
				log(String.format("taskList.Add(new Task(%d, %d, %d, %d));", oIndex, wIndex, o.getDuration(), rIndex));
			}
			log("myJobList.Add(taskList);");
			log("");
		}
		
	}
	*/
	
	private void evaluateResource(Schedule sch,String resCode) {
		
		class WorkOrderPaint{
			long StartDate;
			String Epoxy;
			Operation Op;
		}
		
		Resource SELRES = null;
		for(Resource r : sch.getResourceList()){
			if(resCode.equals(r.getCode()))
				SELRES = r;
		}
		
		
		
		
		SortedArrayList<Long> startDates = new SortedArrayList<Long>(new Comparator<Long>() {
			@Override
			public int compare(Long t1, Long t2) {
				return (int)(t1 - t2);
			}
		});
		Map<Long, List<WorkOrderPaint>> opsPerDate = new HashMap<Long, List<WorkOrderPaint>>();
		List<String> epoxies = new ArrayList<>();
		List<Color> epoxyColors = new ArrayList<>();
		List<WorkOrder> allWo = sch.getWorkOrderList();
		for(WorkOrder wo : allWo){
			List<Operation> ops = wo.getOperations();
			boolean isPaint = false;
			long startDate = 0;
			String epoxy = "*";
			Operation po = null;
			for(Operation o : ops){
				isPaint = o.getResource().equals(SELRES);
				startDate = o.getStartDate();
				startDate = (startDate / (24*3600))*(24*3600); //discard time
				for(StockItemTransaction tr : wo.getRequiredTransaction()){
					if(tr.getItem().getReocod() != 3){//not a produced article so must by epoxy
						epoxy = tr.getItem().getReference();
					}
				}
				po = o;
			}
			if(isPaint){
				WorkOrderPaint p = new WorkOrderPaint();
				p.StartDate = startDate;
				p.Epoxy = epoxy;
				p.Op = po;
				if(!startDates.contains(startDate))
					startDates.add(startDate);
				List<WorkOrderPaint> dateOps = opsPerDate.get(startDate);
				if(dateOps == null){
					dateOps = new ArrayList<WorkOrderPaint>();
					opsPerDate.put(startDate, dateOps);
				}
				dateOps.add(p);
				
				if(!epoxies.contains(epoxy)){
					//int R = (int)(Math.random()*256);
					//int G = (int)(Math.random()*256);
					//int B= (int)(Math.random()*256);
					//Color color = new Color(R, G, B); //random color, but can be bright or dull

					epoxies.add(epoxy);
					//to get rainbow, pastel colors
					Random random = new Random();
					Color color = null;
					do{
						final float hue = random.nextFloat();
						final float saturation = 0.9f;//1.0 for brilliant, 0.0 for dull
						final float luminance = 1.0f; //1.0 for brighter, 0.0 for black
						color = Color.getHSBColor(hue, saturation, luminance);
					}while(epoxyColors.contains(color));
					epoxyColors.add(color);
				}
				
			}
		}
		allWo = null;
		
		long minDate = startDates.get(0);
		int w = 24*3600;// (int)(startDates.get(startDates.size() - 1) - startDates.get(0));
		int barHeight = 100;
		int h = epoxies.size()*barHeight;
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
		
		BufferedImage bImg = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
	    Graphics2D cg = bImg.createGraphics();
	    
	    for(int i = 0; i < startDates.size(); i++){
			Long t = startDates.get(i);
			List<WorkOrderPaint> dateOps = opsPerDate.get(t);
			if(dateOps == null)
				continue;
			
			
			
			cg.setColor(Color.WHITE);
	    	cg.fillRect(0, 0, w, h);
	    	List<WorkOrderPaint> ops = opsPerDate.get(t);
	    	for(WorkOrderPaint p : ops){
	    		int x = (int)(p.Op.getStartDate() - t);
	    		int y = epoxies.indexOf(p.Epoxy);
	    		Color c = epoxyColors.get(y);
	    		y *= barHeight;
	    		cg.setColor(c);
	    		cg.fillRect(x, y, p.Op.getDuration(), barHeight);
	    		cg.setColor(Color.BLACK);
	    		cg.drawLine(x, y, x, y+barHeight);
	    	}
	    	
	    	bImg.flush();
	    	Instant dt = Instant.ofEpochSecond(t);
	    	String fName = "./out/"+ formatter.format(dt)+".png";
	    	try {
    	    	if (ImageIO.write(bImg, "png", new File(fName))){
    	    		System.out.println("-- saved epoxy usage image");
    	    	}
    	    } catch (IOException e) {
    	            // TODO Auto-generated catch block
    	            e.printStackTrace();
    	    }
	    }
	    
    	cg.dispose();
    	bImg = null;
	    
	    
	    
		
		
		for(int i = 0; i < startDates.size(); i++){
			Long t = startDates.get(i);
			List<WorkOrderPaint> dateOps = opsPerDate.get(t);
			if(dateOps == null)
				continue;
			Map<String, Double> loadsPerEpx = new HashMap<String, Double>();
			double loadT = 0, loadTMin = 0;
			for(WorkOrderPaint p : dateOps){
				loadT += p.Op.getDuration();
				Double min = loadsPerEpx.getOrDefault(p.Epoxy, 0.0);
				min += p.Op.getDuration();
				loadsPerEpx.put(p.Epoxy, min);
			}
			StringBuilder sb = new StringBuilder();
			Instant dt = Instant.ofEpochSecond(t);
			//dt.truncatedTo(ChronoUnit.DAYS);
			sb.append(dt.toString());
			sb.append("\t");
			sb.append((int)loadT);
			sb.append("\t");
			for(String epx : loadsPerEpx.keySet()){
				Double m = loadsPerEpx.get(epx);
				sb.append(epx);
				sb.append("("+((int)(double)m)+")");
				sb.append("\t");
			}
			log(sb.toString());
		}
		
	}
	
	
	private void evaluateResourceHTML(Schedule sch,String resCode) throws FileNotFoundException {
		
		class WorkOrderPaint{
			long StartDate;
			String Epoxy;
			Operation Op;
		}
		
		Resource SELRES = null;
		for(Resource r : sch.getResourceList()){
			if(resCode.equals(r.getCode()))
				SELRES = r;
		}
		
		
		PrintStream of = new PrintStream("./out/evaluate.html");
		
		of.println("<html><head><link href=\"style.css\" type=\"text/css\" rel=\"stylesheet\"/>"
				+ "<script type='text/javascript' src='https://code.jquery.com/jquery-3.3.1.min.js'></script>"
				+ "<script type='text/javascript' src='script.js'></script></head><body>");
		
		Score score = sch.getScore();
		of.println("<h1 class=\"score\">"+score.toString()+"</h1>");
		
		
		SortedArrayList<Long> startDates = new SortedArrayList<Long>(new Comparator<Long>() {
			@Override
			public int compare(Long t1, Long t2) {
				return (int)(t1 - t2);
			}
		});
		Map<Long, List<WorkOrderPaint>> opsPerDate = new HashMap<Long, List<WorkOrderPaint>>();
		List<String> epoxies = new ArrayList<>();
		List<Color> epoxyColors = new ArrayList<>();
		List<WorkOrder> allWo = sch.getWorkOrderList();
		for(WorkOrder wo : allWo){
			List<Operation> ops = wo.getOperations();
			boolean isPaint = false;
			long startDate = 0;
			String epoxy = "*";
			Operation po = null;
			for(Operation o : ops){
				isPaint = o.getResource().equals(SELRES);
				startDate = o.getStartDate();
				startDate = (startDate / (24*3600))*(24*3600); //discard time
				for(StockItemTransaction tr : wo.getRequiredTransaction()){
					if(tr.getItem().getReocod() != 3){//not a produced article so must by epoxy
						epoxy = tr.getItem().getReference();
					}
				}
				po = o;
			}
			if(isPaint){
				WorkOrderPaint p = new WorkOrderPaint();
				p.StartDate = startDate;
				p.Epoxy = epoxy;
				p.Op = po;
				if(!startDates.contains(startDate))
					startDates.add(startDate);
				List<WorkOrderPaint> dateOps = opsPerDate.get(startDate);
				if(dateOps == null){
					dateOps = new ArrayList<WorkOrderPaint>();
					opsPerDate.put(startDate, dateOps);
				}
				dateOps.add(p);
				
				if(!epoxies.contains(epoxy)){
					//int R = (int)(Math.random()*256);
					//int G = (int)(Math.random()*256);
					//int B= (int)(Math.random()*256);
					//Color color = new Color(R, G, B); //random color, but can be bright or dull

					epoxies.add(epoxy);
					//to get rainbow, pastel colors
					Random random = new Random();
					Color color = null;
					do{
						final float hue = random.nextFloat();
						final float saturation = 0.9f;//1.0 for brilliant, 0.0 for dull
						final float luminance = 1.0f; //1.0 for brighter, 0.0 for black
						color = Color.getHSBColor(hue, saturation, luminance);
					}while(epoxyColors.contains(color));
					epoxyColors.add(color);
				}
				
			}
		}
		allWo = null;
		
		long minDate = startDates.get(0);
		double w = 24*3600;// (int)(startDates.get(startDates.size() - 1) - startDates.get(0));
		int barHeight = 10;
		int h = epoxies.size()*barHeight;
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
		
	    for(int i = 0; i < startDates.size(); i++){
			Long t = startDates.get(i);
			List<WorkOrderPaint> dateOps = opsPerDate.get(t);
			if(dateOps == null)
				continue;
			
			Instant dt = Instant.ofEpochSecond(t);
			of.println("<h1>"+formatter.format(dt)+"</h1>");
			of.println("<div style=\"position:relative;width:100%;height:"+h+"\">");
			
			for(int r = 0; r < epoxies.size(); r++ ){
				String res = epoxies.get(r);
				int x = 0;
	    		int y = r;
	    		y *= barHeight;
	    		of.println("<small class=\"resource "+res+"\" title=\""+res+"\" style=\"position:absolute;top:"+y+";left:0;height:"+barHeight+";\">"+res+"</small>");
				
			}
			
	    	List<WorkOrderPaint> ops = opsPerDate.get(t);
	    	for(WorkOrderPaint p : ops){
	    		int x = (int)(p.Op.getStartDate() - t);
	    		int y = epoxies.indexOf(p.Epoxy);
	    		Color c = epoxyColors.get(y);
	    		y *= barHeight;
	    		String colorStr = String.format("#%02x%02x%02x", c.getRed(), c.getGreen(), c.getBlue());  
	    		of.println("<div title=\""+p.Op.getCode()+"\" style=\"position:absolute;top:"+y+";left:"+(100*x/w)+"%;width:"+(100*p.Op.getDuration()/w)+"%;height:"+barHeight+";background-color:"+colorStr+"\">&nbsp;</div>");
	    	}
	    	
	    	of.println("</div>");
	    	
	    	
	    }
	    

		
		of.println("</body></html>");
		of.close();
	}
	

	private void evaluateVCRNUMS(Schedule sch) {
		
		List<Operation> ops = sch.getOperationList();
		Map<String, List<WorkOrder>> invoiceMap = new HashMap<String, List<WorkOrder>>();
		HashMap<Operation, String> operationVCR = new HashMap<>();
		List<WorkOrder> allWo = sch.getWorkOrderList();
		for(WorkOrder wo  : allWo){
			for (StockItemProductionTransaction stockProduction : wo.getProducedTransactionList()) {
				List<WorkOrder> woList = invoiceMap.get(stockProduction.getVCR());
				if(woList == null){
					woList = new LinkedList<>();
					invoiceMap.put(stockProduction.getVCR(), woList);
				}
				woList.add(wo);
				for(Operation o : wo.getOperations()){
					operationVCR.put(o, stockProduction.getVCR());
				}
			}
		}
		
		
		
		Map<String, Color> vcrnumColors = new HashMap<>();
		for(String vcr : invoiceMap.keySet()){
			//int R = (int)(Math.random()*256);
			//int G = (int)(Math.random()*256);
			//int B= (int)(Math.random()*256);
			//Color color = new Color(R, G, B); //random color, but can be bright or dull

			//to get rainbow, pastel colors
			Random random = new Random();
			Color color = null;
			do{
				final float hue = random.nextFloat();
				final float saturation = 0.9f;//1.0 for brilliant, 0.0 for dull
				final float luminance = 1.0f; //1.0 for brighter, 0.0 for black
				color = Color.getHSBColor(hue, saturation, luminance);
			}while(vcrnumColors.containsValue(color));
			vcrnumColors.put(vcr, color);
		}
		
		
		SortedArrayList<Long> dates = new SortedArrayList<>(new Comparator<Long>(){

			@Override
			public int compare(Long l1, Long l2) {
				return l1.compareTo(l2);
			}
			
		});
		
		Map<Long, List<Operation>> opsPerDate = new HashMap<>();
		List<Resource> usedResource = new ArrayList<>();
		for(Operation o : ops){
			long t = (o.getStartDate() / (24*3600))*24*3600;
			List<Operation> l = opsPerDate.get(t);
			if(l == null){
				l = new LinkedList<Operation>();
				opsPerDate.put(t, l);
			}
			l.add(o);
			if(!usedResource.contains(o.getResource()))
				usedResource.add(o.getResource());
		}
		
		dates.addAll(opsPerDate.keySet());
		
		
		long minDate = dates.get(0);
		int w = 24*3600;// (int)(startDates.get(startDates.size() - 1) - startDates.get(0));
		int barHeight = 200;
		int h = usedResource.size()*barHeight;
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
		
		BufferedImage bImg = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
	    Graphics2D cg = bImg.createGraphics();
	    
	    for(int i = 0; i < dates.size(); i++){
			Long t = dates.get(i);
			List<Operation> dateOps = opsPerDate.get(t);
			if(dateOps == null)
				continue;
			
			
			
			cg.setColor(Color.WHITE);
	    	cg.fillRect(0, 0, w, h);
	    	for(Operation p : dateOps){
	    		int x = (int)(p.getStartDate() - t);
	    		int y = usedResource.indexOf(p.getResource());
	    		Color c = vcrnumColors.get(operationVCR.get(p));
	    		y *= barHeight;
	    		cg.setColor(c);
	    		cg.fillRect(x, y, p.getDuration(), barHeight);
	    		cg.setColor(Color.BLACK);
	    		cg.drawLine(x, y, x, y+barHeight);
	    	}
	    	
	    	bImg.flush();
	    	Instant dt = Instant.ofEpochSecond(t);
	    	String fName = "./out/"+ formatter.format(dt)+".png";
	    	try {
    	    	if (ImageIO.write(bImg, "png", new File(fName))){
    	    		System.out.println("-- saved epoxy usage image");
    	    	}
    	    } catch (IOException e) {
    	            // TODO Auto-generated catch block
    	            e.printStackTrace();
    	    }
	    }
	    
    	cg.dispose();
    	bImg = null;
	    
	    
	    
		
	}
	
	private void evaluateVCRNUMS_HTML(Schedule sch) throws FileNotFoundException {
		
		
		PrintStream of = new PrintStream("./out/vcrnums.html");
		of.println("<html><head><meta charset=\"utf-8\"><link href=\"style.css\" type=\"text/css\" rel=\"stylesheet\"/>"
				+ "<script type='text/javascript' src='https://code.jquery.com/jquery-3.3.1.min.js'></script>"
				+ "<script type='text/javascript' src='script.js'></script></head><body>");
		
		Score score = sch.getScore();
		of.println("<h1 class=\"score\">"+score.toString()+"</h1>");
		
		
		List<Operation> ops = sch.getOperationList();
		Map<String, List<WorkOrder>> invoiceMap = new HashMap<String, List<WorkOrder>>();
		HashMap<Operation, String> operationVCR = new HashMap<>();
		List<WorkOrder> allWo = sch.getWorkOrderList();
		for(WorkOrder wo  : allWo){
			for (StockItemProductionTransaction stockProduction : wo.getProducedTransactionList()) {
				List<WorkOrder> woList = invoiceMap.get(stockProduction.getVCR());
				if(woList == null){
					woList = new LinkedList<>();
					invoiceMap.put(stockProduction.getVCR(), woList);
				}
				woList.add(wo);
				for(Operation o : wo.getOperations()){
					operationVCR.put(o, stockProduction.getVCR());
				}
			}
		}
		
		
		
		Map<String, Color> vcrnumColors = new HashMap<>();
		for(String vcr : invoiceMap.keySet()){
			//int R = (int)(Math.random()*256);
			//int G = (int)(Math.random()*256);
			//int B= (int)(Math.random()*256);
			//Color color = new Color(R, G, B); //random color, but can be bright or dull

			//to get rainbow, pastel colors
			Random random = new Random();
			Color color = null;
			do{
				final float hue = random.nextFloat();
				final float saturation = 0.9f;//1.0 for brilliant, 0.0 for dull
				final float luminance = 1.0f; //1.0 for brighter, 0.0 for black
				color = Color.getHSBColor(hue, saturation, luminance);
			}while(false && vcrnumColors.containsValue(color));
			vcrnumColors.put(vcr, color);
		}
		
		
		SortedArrayList<Long> dates = new SortedArrayList<>(new Comparator<Long>(){

			@Override
			public int compare(Long l1, Long l2) {
				return l1.compareTo(l2);
			}
			
		});
		
		Map<Long, List<Operation>> opsPerDate = new HashMap<>();
		List<Resource> usedResource = new ArrayList<>();
		for(Operation o : ops){
			long t = (o.getStartDate() / (24*3600))*24*3600;
			List<Operation> l = opsPerDate.get(t);
			if(l == null){
				l = new LinkedList<Operation>();
				opsPerDate.put(t, l);
			}
			l.add(o);
			if(!usedResource.contains(o.getResource()))
				usedResource.add(o.getResource());
		}
		
		dates.addAll(opsPerDate.keySet());
		
		
		long minDate = dates.get(0);
		double w = 24*3600;// (int)(startDates.get(startDates.size() - 1) - startDates.get(0));
		int barHeight = 10;
		int h = usedResource.size()*barHeight;
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd (EEE)").withZone(ZoneId.of("UTC"));
		
		SortedArrayList<Operation> sorter = new SortedArrayList<>(new Comparator<Operation>(){

			@Override
			public int compare(Operation o1, Operation o2) {
				return o1.getStartDate().compareTo(o2.getStartDate());
			}
			
		});
		
	    for(int i = 0; i < dates.size(); i++){
			Long t = dates.get(i);
			List<Operation> dateOps = opsPerDate.get(t);
			if(dateOps == null)
				continue;
			
			Instant dt = Instant.ofEpochSecond(t);
			of.println("<h1>"+formatter.format(dt)+"</h1>");
			of.println("<p class=\"time\" style=\"position:relative;width:100%;height:10\">");
			for(int hour = 0; hour < 24; hour++){
				String hStr = String.format("%02d:00", hour);
				of.println("<span style=\"position:absolute;top:0;left:"+(100*hour*3600/w)+"%;height:10;\">"+hStr+"</span>");
			}
			of.println("</p>");
			

			of.println("<div class=\"day\" style=\"position:relative;width:100%;height:"+h+"\">");
			
			for(int r = 0; r < usedResource.size(); r++ ){
				Resource res = usedResource.get(r);
				int x = 0;
	    		int y = r;
	    		y *= barHeight;
	    		of.println("<small class=\"resource "+res.getCode()+" "+(r%2==0 ? "even": "odd")+"\" title=\""+res.getCode()+"\" style=\"position:absolute;top:"+y+";left:0;height:"+barHeight+";\">"+res.getCode()+"</small>");
				
			}
			
			sorter.clear();
			sorter.addAll(dateOps);
			
	    	for(Operation p : sorter){
	    		String vcr =  operationVCR.get(p);
	    		int x = (int)(p.getStartDate() - t);
	    		int y = usedResource.indexOf(p.getResource());
	    		Color c = vcrnumColors.get(vcr);
	    		y *= barHeight;
	    		String colorStr = String.format("#%02x%02x%02x", c.getRed(), c.getGreen(), c.getBlue());  
	    		of.println("<div class=\"operation\" data-mfgnum=\""+vcr+"\" title=\""+p.getCode()+"\" style=\"position:absolute;top:"+y+";left:"+(100*x/w)+"%;width:"+(100*p.getDuration()/w)+"%;height:"+barHeight+";background-color:"+colorStr+"\">&nbsp;</div>");

	    	}
	    	
	    	of.println("</div>");
	    }
	    
	    
    	of.println("</body></html>");
		of.close();
		
	}
	
	private void evaluateLoadPerDay(Schedule sch) {
		
		Map<Resource, List<Operation>> resourceOperations = new HashMap<>();
		List<Resource> resources = sch.getResourceList();
		List<Operation> operations = sch.getOperationList();
		
		for(Operation o : operations)
		{
			Resource res = o.getResource();
			List<Operation> list = resourceOperations.get(res);
			if(list == null){
				list = new SortedArrayList<Operation>(new Comparator<Operation>() {
					@Override
					public int compare(Operation o1, Operation o2) {
						return (int)((o1.getStartDate() - o2.getStartDate()));
					}
				});
				resourceOperations.put(res, list);
			}
			list.add(o);
		}
		
		
		for(Resource res : resources){
			List<Operation> list = resourceOperations.get(res);
			if(list == null){
				log("No loads for "+res.toString());
				continue;
			}
			Map<Long, Double> load = new HashMap<>();
			List<Long> dates = new ArrayList<>();
			for(Operation o : list){
				long start = (o.getStartDate() / (24*3600))*24*3600;
				double l = load.getOrDefault(start, 0.0);
				l += o.getDuration();
				load.put(start, l);
				if(!dates.contains(start))
					dates.add(start);
			}
			
			
			StringBuilder sb = new StringBuilder();
			sb.append(res.toString());
			sb.append("\t");
			for(long start : dates){
				
				sb.append(load.get(start));
				sb.append("\t");	
			}
			

			log(sb.toString());
			
			
			
		}
		
		
		
		
		
		
	}
	
	private void evaluateStockRupture(Schedule sch){
		
		
		List<WorkOrder> allWos = sch.getWorkOrderList();
		
		Map<StockItem, StockItemCapacityTracker> mProducedItemsStock = new HashMap<>();
		//first step get to know produced items
		for(int i = allWos.size() - 1; i >= 0; i--){
			WorkOrder wo = allWos.get(i);
			StockItemTransaction trans = wo.getProducedTransactionList().get(0); 
			StockItem item = trans.getItem();
			StockItemCapacityTracker tr = mProducedItemsStock.get(item);
			if(tr == null){
				tr = new StockItemCapacityTracker(item, 0);
				mProducedItemsStock.put(item, tr);
			}
			tr.insertProduction(trans, wo);
		}
		//based on the items recorded we now setup consumptions
		for(int i = allWos.size() - 1; i >= 0; i--){
			WorkOrder wo = allWos.get(i);
			List<StockItemTransaction> requirements = wo.getRequiredTransaction();
			for(StockItemTransaction trans : requirements){
				StockItem item = trans.getItem();
				StockItemCapacityTracker tr = mProducedItemsStock.get(item);
				if(tr != null){
					//indeed this is a produced item
					tr.insertRequirement(trans, wo);
					if(tr.getHardScore() < 0){
						Instant dt = Instant.ofEpochSecond(wo.getFirstStart());
						log("Stock break ("+(tr.getHardScore()/10000)+") for "+item.getReference()+" at "+dt.toString());
					}
				}
			}
		}
		
	}
	
	
	
	private void log(String message){
		//System.out.println(Thread.currentThread().getId()+" > " +  message);
		System.out.println(message);
	}

}
