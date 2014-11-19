package cws.core.algorithms;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import org.cloudbus.cloudsim.Log;

import cws.core.VM;
import cws.core.core.VMType;

import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;

import cws.core.engine.Environment;
import cws.core.cloudsim.CloudSimWrapper;



/**
 * TODO: implement storage aware version (i.e. non-zero data transfer time).
 *       agents as a member var?
 */
public class Heft extends StaticAlgorithm {

    List<VM> vms;

    public Heft(double budget, double deadline,
                Environment environment, CloudSimWrapper cloudsim,
                List<VM> vms) {
        super(budget, deadline, null, null, environment, cloudsim);

        this.vms = vms;
    }


    public Plan planDAG(DAG dag, Plan currentPlan) throws NoFeasiblePlan {
        return new Plan();
    }


    // Get computation time for a job on a vm.
    public double compCost(Task job, VM vm) {
        return job.getSize() / vm.getVmType().getMips();
    }


    // Get average (over all VMs) of the time taken for this task.
    public double wBar(Task task) {
        double total = 0.0;
        for(VM vm : this.vms) {
            total += compCost(task, vm);
        }
        return total / this.vms.size();
    }


    /**
     * Get average communication cost of the variables transferred between
     * jobs t1 and t2 between all pairs of VMs. NOT IMPLEMENTED
     */
    public double cBar(Task t1, Task t2) {

        // // Catch simple case
        // if(this.vms.size() == 1) { return 0.0; }

        // double sum = 0.0;
        // int count = 0;

        // for(VM vma :  this.vms) {
        //     for(VM vmb : this.vms) {
        //         if(!vma.equals(vmb)) {
        //             sum += getCommCost(t1, t2, vma, vmb);
        //             count++;
        //         }
        //     }
        // }

        // return sum / ((double) count);

        // All transfers are free! (for now)
        return 0.0;
    }


    /**
     * Compute rank of a job, based on computation time, rank of successors
     * and transfer time to successors.
     */
    public double rankU(Task task) {

        // TODO: optimise? Could memoise calculations when calculating rank
        // for all tasks?

        double max_child_rank = 0.0;

        // If we have children then recurse and calculate their max rank
        List<Task> children = task.getChildren();
        if(children.size() > 0) {
            for(Task child : children) {
                double crank = rankU(child) + cBar(child, task);
                max_child_rank = Math.max(crank, max_child_rank);
            }
        }

        return max_child_rank + wBar(task);
    }


    /**
     * Get list of tasks in a DAG ordered by rankU().
     */
    public List<Task> rankedTasks(DAG dag) {

        // Get tasks (this code is crap but I don't want to change the core
        // library too much so we're stuck with it...)
        List<Task> sorted = new ArrayList<Task>();
        for(String id : dag.getTasks()) {
            sorted.add(dag.getTaskById(id));
        }

        // Construct sort-by-rankU function
        final Heft heft = this;
        Comparator<Task> compare = new Comparator<Task>() {
            @Override
            public int compare(Task t1, Task t2) {
                double r1 = heft.rankU(t1);
                double r2 = heft.rankU(t2);

                return Double.compare(r1, r2);
            }
        };

        // Sort the list, actually we want it reversed (highest rank
        // first).
        Collections.sort(sorted, Collections.reverseOrder(compare));

        return sorted;
    }




}
