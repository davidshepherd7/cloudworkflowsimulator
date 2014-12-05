package cws.core.algorithms.heterogeneous;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import java.util.List;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.HashSet;


import cws.core.VM;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.VMFactory;
import cws.core.provisioner.ConstantDistribution;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;
import cws.core.dag.algorithms.TopologicalOrder;

import cws.core.dag.Task;
import cws.core.dag.DAG;

import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;

/**
 * Helper functions for testing planner classes
 *
 * @author David Shepherd
 */
public class PlannerTestBase {
    
    public VMType makeVM(double mips) {
        return VMTypeBuilder.newBuilder().mips(mips).
                cores(1).price(1.0)
                .provisioningTime(new ConstantDistribution(0.0))
                .deprovisioningTime(new ConstantDistribution(0.0))
                .build();
    }

    public List<VMType> makeUniformVMS(int nVM) {

        // 3 identical VMs
        List<VMType> vms = new ArrayList<>();
        for(int i=0; i<nVM; i++) {
            vms.add(makeVM(1.0));
        }
        return vms;
    }

    public List<VMType> makeNonUniformVMS() {

        // One each of three different VMs
        List<VMType> vms = new ArrayList<>();
        vms.add(makeVM(0.5));
        vms.add(makeVM(0.25));
        vms.add(makeVM(1.0));

        return vms;
    }


    public DAG makeTasks() {
        // dag = {a: (c,),
        //        b: (d,),
        //        c: (e,),
        //        d: (e,),
        //        e: (f, g)}

        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 1.0));
        dag.addTask(new Task("b", "", 1.1));
        dag.addTask(new Task("c", "", 1.0));
        dag.addTask(new Task("d", "", 1.1));
        dag.addTask(new Task("e", "", 1.0));
        dag.addTask(new Task("f", "", 1.0));
        dag.addTask(new Task("g", "", 1.5)); // break the f vs g symmetry

        dag.addEdge("a", "c");
        dag.addEdge("b", "d");
        dag.addEdge("c", "e");
        dag.addEdge("d", "e");
        dag.addEdge("e", "f");
        dag.addEdge("e", "g");

        return dag;
    }

    public DAG makeDiamondDAG() {
        // Diamond DAG
        DAG dag = new DAG();
        dag.addTask(new Task("0", "", 10));
        dag.addTask(new Task("1", "", 11));
        dag.addTask(new Task("2" , "", 12));
        dag.addTask(new Task("3" , "", 13));
        dag.addEdge("0", "1");
        dag.addEdge("0", "2");
        dag.addEdge("1", "3");
        dag.addEdge("2", "3");

        return dag;
    }

    public DAG makeSingleTaskDAG() {
        DAG dag = new DAG();
        dag.addTask(new Task("a", "", 1));

        return dag;
    }

    public void assertAllTasksArePlanned(Plan plan, DAG dag) {

        // Pull out task list via ordering because it's much easier
        TopologicalOrder order = new TopologicalOrder(dag);
        Set<Task> allTasks = new HashSet<Task>();
        for(Task t : order) {
            allTasks.add(t);
        }

        assertAllTasksArePlanned(plan, allTasks);
    }

    public void assertAllTasksArePlanned(Plan plan, Collection<Task> tasks) {

        Set<Task> planned = new HashSet<Task>();
        for(Resource r : plan.resources) {
            for(Slot s : r.getSlots()) {
                planned.add(s.task);
            }
        }

        // Comparing this way gives better failure output that
        // assert(a.equals(b)) etc.
        assertThat(planned, hasItems(tasks.toArray(new Task[0])));
    }

    // Helper code to compare plans
    // ============================================================

    // Since we aren't allow to implement .equals for Task we have to copy
    // the plan information to a new object which does implement .equals,
    // then compare the new objects.

    // I'm going to ignore the fact that two tasks from different DAGs can
    // have the same id for now, because it seems to be impossible to work
    // around here (maybe Task should store a DAG id as well?).


    public void assertSamePlans(Plan actual, Plan expected) {
        assertThat(simplifyPlan(actual), is(simplifyPlan(expected)));
    }


    /** Convert a Plan to a simpler object for comparison purposes */
    private List<SimplePlanEntry> simplifyPlan(Plan plan) {

        List<SimplePlanEntry> l = new ArrayList<>();
        for(Resource r : plan.resources) {
            for(Slot s : r.getSlots()) {
                SimplePlanEntry entry = new SimplePlanEntry(r.vmtype,  s.task.getId(),
                        s.start, s.duration);
                l.add(entry);
            }
        }

        // Sort by task id for ease of comparison.
        Comparator<SimplePlanEntry> compare = new Comparator<SimplePlanEntry>() {
            @Override
            public int compare(SimplePlanEntry a, SimplePlanEntry b) {
                return a.taskId.compareTo(b.taskId);
            }
        };

        Collections.sort(l, compare);

        return l;
    }

    private static class SimplePlanEntry {

        public final VMType vmtype;
        public final String taskId;
        public final double start;
        public final double duration;

        SimplePlanEntry(VMType vmtype, String taskId, double start, double duration) {
            this.vmtype = vmtype;
            this.taskId = taskId;
            this.start = start;
            this.duration = duration;
        }

        @Override
        public boolean equals(Object other) {
            if(! (other instanceof SimplePlanEntry)) {
                return false;
            }
            else {
                SimplePlanEntry otherT = (SimplePlanEntry) other;
                return otherT.taskId == taskId
                        && otherT.vmtype == vmtype
                        && fpEqual(otherT.start, start)
                        && fpEqual(otherT.duration, duration);
            }
        }

        // Hash sets of this object not implemented. Possible issues with
        // floating point comparison in .equals().
        @Override
        public int hashCode() {
            throw new UnsupportedOperationException("Hash code not implemented.");
        }

        @Override
        public String toString() {
            return String.format("Entry(%s, %s, %f, %f)",
                    vmtype, taskId, start, duration);
        }

        private boolean fpEqual(double a , double b) {
            final double tol = 1e-10;
            return Math.abs(a - b) < tol;
        }
    }

}
