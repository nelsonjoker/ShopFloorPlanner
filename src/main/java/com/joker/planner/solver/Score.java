package com.joker.planner.solver;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Score implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4036630775872811788L;

	private static final Pattern theScoreRegex = Pattern.compile("\\[([0-9/\\-]+)\\]hard/\\[([0-9/\\-]+)\\]soft", Pattern.CASE_INSENSITIVE); 
	
	private long[] hardScores;
	private long[] softScores;
	
	public Score(){
		hardScores = null;
		softScores = null;
	}
	public Score(Score copy){
		this(copy.hardScores, copy.softScores);
	}
	private Score(long[] hard, long[] soft){
		hardScores = new long[hard.length];
		softScores = new long[soft.length];
		System.arraycopy(hard, 0, hardScores, 0, hard.length);
		System.arraycopy(soft, 0, softScores, 0, soft.length);
	}
	
	public Score multiply(double x){
		Score res = new Score(this);
		for(int i = 0; i < hardScores.length; i++){
			res.hardScores[i] *= x;
		}
		for(int i = 0; i < softScores.length; i++){
			res.softScores[i] *= x;
		}
		return res;
	}

	public Score subtract(Score score) {
		Score res = new Score(this);
		for(int i = 0; i < hardScores.length; i++){
			res.hardScores[i] -= score.hardScores[i];
		}
		for(int i = 0; i < softScores.length; i++){
			res.softScores[i] -= score.softScores[i];
		}
		return res;
	}


	public Score add(Score score) {
		Score res = new Score(this);
		for(int i = 0; i < hardScores.length; i++){
			res.hardScores[i] += score.hardScores[i];
		}
		for(int i = 0; i < softScores.length; i++){
			res.softScores[i] += score.softScores[i];
		}
		return res;
	}


	
	public int compareTo(Score other){
		return compareTo(other, true,true);
	}

	public int compareTo(Score other, boolean hard, boolean soft){
		long c = 0;
		if(hard){
			for(int i = 0; i < hardScores.length; i++){
				c = hardScores[i] - other.hardScores[i];
				if(c != 0)
					return (int)c;
			}
		}
		if(soft){
			for(int i = 0; i < softScores.length; i++){
				c = softScores[i] - other.softScores[i];
				if(c != 0)
					return (int)c;
			}
		}
		return 0;
	}

	public static Score parseScore(String str) {
		
		Matcher m = theScoreRegex.matcher(str);
		if(m.matches()){
			
			String hard = m.group(1);
			String soft = m.group(2);
			
			String[] els = hard.split("/");
			long[] hardScores = new long[els.length];
			for(int i = 0; i < els.length; i++){
				hardScores[i] = Long.parseLong(els[i]);
			}
			els = soft.split("/");
			long[] softScores = new long[els.length];
			for(int i = 0; i < els.length; i++){
				softScores[i] = Long.parseLong(els[i]);
			}
			
			return Score.valueOf(hardScores, softScores);
		}
		return null;
	}


	public static Score valueOf(long[] hard, long[] soft) {
		return new Score(hard, soft);
	}
	public static Score valueOf(int hard, int soft) {
		return new Score(new long[] { hard } , new long[] { soft });
	}
	
	public long[] values(){
		long[] v = new long[hardScores.length + softScores.length];
		int s = 0;
		for(int i = 0 ; i < hardScores.length; i++)
			v[s++] = hardScores[i];
		for(int i = 0 ; i < softScores.length; i++)
			v[s++] = softScores[i];
		
		return v;
	}


	public long getHardScore() {
		long sum = 0;
		for(int i = 0; i < hardScores.length; i++)
			sum += hardScores[i];
		return sum;
	}


	public long getSoftScore() {
		long sum = 0;
		for(int i = 0; i < softScores.length; i++)
			sum += softScores[i];
		return sum;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for(int i = 0; i < hardScores.length; i++){
			sb.append(hardScores[i]);
			if(i < hardScores.length-1)
				sb.append('/');
		}
		sb.append("]hard/");
		sb.append('[');
		for(int i = 0; i < softScores.length; i++){
			sb.append(softScores[i]);
			if(i < softScores.length-1)
				sb.append('/');
		}
		sb.append("]soft");
		return sb.toString();
	}
	
}
