package cws.core.algorithms.heterogeneous;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map; 

import org.cloudbus.cloudsim.Log;

import cws.core.VM;
import cws.core.core.VMType;

import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;

import cws.core.engine.Environment;
import cws.core.cloudsim.CloudSimWrapper;

import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;



/**
 * TODO: implement storage aware version (i.e. non-zero data transfer time).
 *      
 */
public class HeftPlanner implements Planner {

    @Override
    public Plan planDAG(DAG dag, List<VMType> availableVMTypes) throws NoFeasiblePlan {
        return new Plan();
    }

    // Get computation time for a job on a vm.
    public static double compCost(Task job, VMType vmtype) {
        return job.getSize() / vmtype.getMips();
    }

    // Get average (over all VMs) of the time taken for this task.
    public static double meanComputationTime(Task task, Map<VMType, Integer> nvms) {
        double total = 0.0;
        int count = 0;
        for(Map.Entry<VMType, Integer> entry : nvms.entrySet()) {
            total += compCost(task, entry.getKey()) * entry.getValue();
            count += entry.getValue();
        }
        return total / count;
    }


    /**
     * Get average communication cost of the variables transferred between
     * jobs t1 and t2 between all pairs of VMs. NOT IMPLEMENTED
     */
    public static double cBar(Task t1, Task t2) {
        throw new UnsupportedOperationException("communication times not implemented."); 
    }


    /**
     * Compute rank of a job, based on computation time, rank of successors
     * and transfer time to successors.
     */
    public static double upwardRank(Task task, Map<VMType, Integer> vmNumbers) {

        double max_child_rank = 0.0;

        // If we have children then recurse and calculate their max rank
        List<Task> children = task.getChildren();
        if(children.size() > 0) {
            for(Task child : children) {
                double crank = upwardRank(child, vmNumbers); // + cBar(child, task);
                max_child_rank = Math.max(crank, max_child_rank);
            }
        }

        return max_child_rank + meanComputationTime(task, vmNumbers);
    }


    /**
     * Get list of tasks in a DAG ordered by upwardRank().
     */
    public static List<Task> rankedTasks(DAG dag, Map<VMType, Integer> vmNumbers) {

        // Get tasks (this code is crap but I don't want to change the core
        // library too much so we're stuck with it...)
        List<Task> sorted = new ArrayList<Task>();
        for(String id : dag.getTasks()) {
            sorted.add(dag.getTaskById(id));
        }

        // Get all ranks and store in map
        final Map<Task, Double> ranks = new HashMap<Task, Double>();
        for(Task t : sorted) {
            ranks.put(t, upwardRank(t, vmNumbers));
        } 
        
        // Construct function to sort by rank
        Comparator<Task> compare = new Comparator<Task>() {
            @Override
            public int compare(Task t1, Task t2) {
                return Double.compare(ranks.get(t1), ranks.get(t2));
            }
        };

        // Sort the list by rank, actually we want it reversed (highest
        // rank first).
        Collections.sort(sorted, Collections.reverseOrder(compare));

        return sorted;
    }


}
