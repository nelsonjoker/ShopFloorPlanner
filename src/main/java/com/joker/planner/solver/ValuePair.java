package com.joker.planner.solver;

public class ValuePair<L extends Comparable<R>, R extends Comparable<L>> {

	  private final L left;
	  private final R right;

	  public ValuePair(L left, R right) {
	    this.left = left;
	    this.right = right;
	  }

	  public L getLeft() { return left; }
	  public R getRight() { return right; }

	  /**
	   * checks if two interval values intersect
	   * @see http://world.std.com/~swmcd/steven/tech/interval.html
	   * @param other
	   * @return true if ranges intersect
	   */
	  public boolean intersects(ValuePair<L, R> other){
		  
		  int cp1 = this.getLeft().compareTo(other.getRight());
		  int cp2 = other.getLeft().compareTo(this.getRight());
		  
		  return cp1 <= 0 && cp2 <= 0;
		  
	  }
	  
	  public boolean intersects(L l, R r){
		  
		  int cp1 = this.getLeft().compareTo(r);
		  int cp2 = l.compareTo(this.getRight());
		  
		  return cp1 <= 0 && cp2 <= 0;
		  
	  }
	  
	  @Override
	  public int hashCode() { return left.hashCode() ^ right.hashCode(); }

	  @Override
	  public boolean equals(Object o) {
	    if (!(o instanceof ValuePair)) return false;
	    ValuePair<L, R> pairo = (ValuePair<L, R>) o;
	    return this.left.equals(pairo.getLeft()) && this.right.equals(pairo.getRight());
	  }

	}