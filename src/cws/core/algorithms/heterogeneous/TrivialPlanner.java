package cws.core.algorithms.heterogeneous;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

import cws.core.dag.DAG;
import cws.core.core.VMType;
import cws.core.dag.Task;
import cws.core.dag.algorithms.TopologicalOrder;


import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;


/**
 * Very uninteresting planning algorithm: assigns all tasks to a single VM
 * (in an appropriate order). Useful for testing and to see how to
 * implement a Planner.
 *
 * @author David Shepherd
 */
public class TrivialPlanner implements Planner {
    
    @Override
    public Plan planDAG(DAG dag, Plan currentPlan) throws NoFeasiblePlan {

        // // Pick VM type with most mips
        // Comparator<VMType> vmCompare = new Comparator<VMType>() {
        //     @Override
        //     public int compare(VMType v1, VMType v2) {
        //         return -1 * Double.compare(v1.getMips(), v2.getMips());
        //     }
        // };
        // List<VMType> sorted = currentPlan.vmList();
        // Collections.sort(sorted, vmCompare);
        // VMType vmType = sorted.get(0);

        // Pick the resource with the most mips
        Resource rFastest = currentPlan.resources.iterator().next();
        for (Resource r : currentPlan.resources) {
            if (r.vmtype.getMips() > rFastest.vmtype.getMips()) {
                rFastest = r;
            }
        }

        // Tasks must run in a topological order to ensure that parent
        // tasks are always completed before their children.
        TopologicalOrder order = new TopologicalOrder(dag);

        // Assign tasks to the single VM in the topological order, store
        // assignments in a Plan class.
        for(Task t : order) {
            currentPlan.schedule(rFastest, t, rFastest.getEndOfSchedule());
        }

        return currentPlan;
    }

}
