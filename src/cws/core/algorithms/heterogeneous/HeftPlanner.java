package cws.core.algorithms.heterogeneous;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


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
 * Heterogeneous earliest finish time (HEFT) scheduling algorithm. See
 * "Performance-effective and low-complexity task scheduling for
 * heterogeneous computing" (Topcuoglu2002) for details.
 *
 * @author David Shepherd
 *
 * TODO: implement storage/communication aware version (i.e. with non-zero
 * data transfer time).
 *
 */
public class HeftPlanner implements Planner {


    /** The main function of the Planner interface. */
    @Override
    public Plan planDAG(DAG dag, List<VMType> availableVMTypes) throws NoFeasiblePlan {

        //??ds just say we have one of each vm for now...
        Map<VMType, Integer> vmNumbers = new HashMap<>();
        for(VMType vmt : availableVMTypes) {
            vmNumbers.put(vmt, 1);
        }

        List<Task> rankedTasks = rankedTasks(dag, vmNumbers);
        return createPlan(rankedTasks, vmNumbers);
    }


    /** Get computation time for a job on a vm. */
    public static double compCost(Task job, VMType vmtype) {
        return job.getSize() / vmtype.getMips();
    }


    /** Get average (over all VMs) of the time taken for this task. */
    public static double meanComputationTime(Task task, Map<VMType, Integer> vmNumbers) {
        double total = 0.0;
        int count = 0;
        for(Map.Entry<VMType, Integer> entry : vmNumbers.entrySet()) {
            total += compCost(task, entry.getKey()) * entry.getValue();
            count += entry.getValue();
        }
        return total / count;
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
                // Tie breaking is supposed to be random (see H. Topcuoglu
                // 2002 paper) but non-determinism is evil so I'll leave it
                // up to the sorting algorithm to break ties.
                return Double.compare(ranks.get(t1), ranks.get(t2));
            }
        };

        // Sort the list by rank, actually we want it reversed (highest
        // rank first).
        Collections.sort(sorted, Collections.reverseOrder(compare));

        return sorted;
    }


    /** Actually create the plan. */
    public static Plan createPlan(List<Task> rankedTasks, Map<VMType, Integer> vmNumbers)
            throws NoFeasiblePlan {

        // Create list of VM resources available
        List<Resource> vms = new ArrayList<>();
        for(Map.Entry<VMType, Integer> e : vmNumbers.entrySet()) {
            for(int i=0; i<e.getValue(); i++){
                vms.add(new Resource(e.getKey()));
            }
        }


        // Schedule tasks in rank order
        Plan plan = new Plan();
        for(Task t : rankedTasks) {

            // Get earliest possible start time based on parent limitations
            double lastParentFinishTime = 0;
            for(Task p : t.getParents()) {
                double parentFinishTime = plan.getFinishTime(p);
                lastParentFinishTime = Math.max(lastParentFinishTime, parentFinishTime);
            }

            // Check each resource for earliest finish time, and pick the
            // one that gives the earliest of them all.
            double earliestFinishTime = Double.MAX_VALUE;
            Solution bestSolution = null;
            for(Resource r : vms) {

                // Compute times
                final double duration = r.vmtype.getPredictedTaskRuntime(t);
                final double resourceStartTime = Math.max(r.findFirstGap(duration),
                        lastParentFinishTime);
                final double resourceFinishTime = resourceStartTime + duration;

                // If it's better then store it as the new best solution
                if (resourceFinishTime < earliestFinishTime) {
                    bestSolution = new Solution(r,
                            new Slot(t, resourceStartTime, duration),
                            r.vmtype.getVMCostFor(duration),
                            false);
                    earliestFinishTime = resourceFinishTime;
                }
            }

            bestSolution.addToPlan(plan);
        }

        return plan;
    }

}
