package cws.core.algorithms.heterogeneous;

import java.util.List;

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
    public Plan planDAG(DAG dag, List<VMType> availableVMTypes) {

        // Only works for a single VM
        if(availableVMTypes.size() != 1) {
            throw new RuntimeException("Only for a single VM");
        }

        // Create a resource for our one and only VM
        VMType vmType = availableVMTypes.get(0);
        Resource r = new Resource(vmType);

        // Tasks must run in a topological order to ensure that parent
        // tasks are always completed before their children.
        TopologicalOrder order = new TopologicalOrder(dag);

        // Assign tasks to the single VM in the topological order, store
        // assignments in a Plan class.
        Plan plan = new Plan();
        double previous_finish_time = 0.0;
        for(Task t : order) {

            // Compute times
            final double duration = vmType.getPredictedTaskRuntime(t);
            final double start = previous_finish_time;

            // Construct Solution class and add to plan
            Slot slot = new Slot(t, start, duration);
            Solution sol = new Solution(r, slot, 10, false);
            sol.addToPlan(plan);

            // Update
            previous_finish_time += duration;
        }

        return plan;
    }

}
