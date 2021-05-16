package com.joker.planner;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SortedList<E> extends AbstractList<E> {

    private List<E> internalList = new ArrayList<E>();
    private Comparator<? super E> mComparator;

    public SortedList(Comparator<? super E> comparator) {
    	mComparator = comparator;
	}
    
    
    // Note that add(E e) in AbstractList is calling this one
    @Override 
    public void add(int position, E e) {
    	for(position = 0; position < internalList.size(); position++){
    		if(mComparator.compare(e, internalList.get(position)) < 0){
    			break;
    		}
    	}
    	
        internalList.add(position, e);
    }

    @Override
    public E get(int i) {
        return internalList.get(i);
    }

    @Override
    public int size() {
        return internalList.size();
    }
    @Override
    public E remove(int index) {
    	return internalList.remove(index);
    }
    
    
    @Override
    public Iterator<E> iterator() {
    	return internalList.iterator();
    }
}