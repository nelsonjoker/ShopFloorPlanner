package pt.sotubo.planner.solver;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.optaplanner.core.impl.heuristic.selector.common.decorator.SelectionSorterWeightFactory;

import pt.sotubo.planner.domain.Operation;
import pt.sotubo.planner.domain.Resource;
import pt.sotubo.planner.domain.Schedule;


public class ResourceUsageStrengthWeightFactory implements SelectionSorterWeightFactory<Schedule, Resource> {

    public Comparable createSorterWeight(Schedule schedule, Resource res) {
        Map<Resource, Integer> requirementTotalMap = new HashMap<Resource, Integer>(schedule.getResourceList().size());
        
        for (Resource resourceRequirement : schedule.getResourceList()) {
            requirementTotalMap.put(resourceRequirement, 0);            
        }
    	for(Operation op : schedule.getOperationList()){
    		Resource used = op.getResource();
    		Integer total = requirementTotalMap.get(used);
            if (total != null) {
                total += op.getResourceRequirement();
                requirementTotalMap.put(used, total);
            }
    	}

        double requirementDesirability = 0.0;
        int total = requirementTotalMap.get(res);
        if (total > res.getCapacity()) {
        	requirementDesirability += (double) (total - res.getCapacity());
        	/*
            requirementDesirability += (double) (total - res.getCapacity())
                    * (double) resourceRequirement.getRequirement()
                    * (resource.isRenewable() ? 1.0 : 100.0);
            */
         }
        
        return new ResourceUsageStrengthWeight(res, requirementDesirability);
    }

    public static class ResourceUsageStrengthWeight implements Comparable<ResourceUsageStrengthWeight> {

        private final Resource resource;
        private final double requirementDesirability;

        public ResourceUsageStrengthWeight(Resource res, double requirementDesirability) {
            this.resource = res;
            this.requirementDesirability = requirementDesirability;
        }

        public int compareTo(ResourceUsageStrengthWeight other) {
            return new CompareToBuilder()
                    // The less requirementsWeight, the less desirable resources are used
                    .append(requirementDesirability, other.requirementDesirability)
                    .append(resource.getCode(), other.resource.getCode())
                    //.append(executionMode.getId(), other.executionMode.getId())
                    .toComparison();
        }

    }

}
