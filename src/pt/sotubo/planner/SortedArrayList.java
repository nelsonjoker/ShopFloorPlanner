package pt.sotubo.planner;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;



public class SortedArrayList<T> extends ArrayList<T> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private Comparator<? super T> mComparator;
    
    public SortedArrayList(Comparator<? super T> comparator) {
    	mComparator = comparator;
	}
    public SortedArrayList(SortedArrayList<T> copy) {
    	mComparator = copy.mComparator;
	}
    
    
    @Override
    public void add(int index, T element) {
        super.add(index, element);
        Collections.sort(this, mComparator);
        
    }

    @Override
    public boolean add(T element) {
        boolean returnValue = super.add(element);
        Collections.sort(this, mComparator);
        return returnValue;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        boolean returnValue = super.addAll(c);
        Collections.sort(this, mComparator);
        return returnValue;
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        boolean returnValue = super.addAll(index, c);
        Collections.sort(this, mComparator);
        return returnValue;
    }

    @Override
    public T set(int index, T element) {
        T returnValue = super.set(index, element);
        Collections.sort(this, mComparator);
        return returnValue;
    }
}