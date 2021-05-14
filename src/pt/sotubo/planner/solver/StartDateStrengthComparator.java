package pt.sotubo.planner.solver;

import java.io.Serializable;
import java.util.Comparator;

public class StartDateStrengthComparator implements Comparator<Long>, Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3917393701793492971L;

	public int compare(Long a, Long b) {
        return a.compareTo(b);
    }

}
