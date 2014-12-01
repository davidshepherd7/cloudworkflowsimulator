package cws.core.algorithms.heterogeneous;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Comparator;


import cws.core.VM;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.VMFactory;
import cws.core.provisioner.ConstantDistribution;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;


import cws.core.dag.Task;
import cws.core.dag.DAG;

import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;


//??ds pull out some helper functions? eg. dag makers


// Based on https://github.com/mrocklin/heft/blob/master/heft/tests/test_core.py
public class HeftPlannerTest {

    @Before
    public void setUp() {
    }

    public VMType makeVM(double mips) {
        return VMTypeBuilder.newBuilder().mips(mips).
                cores(1).price(1.0)
                .provisioningTime(new ConstantDistribution(0.0))
                .deprovisioningTime(new ConstantDistribution(0.0))
                .build();
    }

    public Map<VMType, Integer> makeUniformVMS(int nVM) {

        // 3 identical VMs
        Map<VMType, Integer> agents = new HashMap<>();
        agents.put(makeVM(1.0), nVM);

        return agents;
    }

    public Map<VMType, Integer> makeNonUniformVMS() {

        // One each of three different VMs
        Map<VMType, Integer> agents = new HashMap<>();
        agents.put(makeVM(0.5), 1);
        agents.put(makeVM(0.25), 1);
        agents.put(makeVM(1.0), 1);

        return agents;
    }


    @Test
    public void testVMEquality() {
        VMType a = makeVM(2.0);
        VMType b = makeVM(2.0);
        VMType c = makeVM(3.0);

        Assert.assertTrue(a.equals(a));
        Assert.assertFalse(a.equals(b));
        Assert.assertFalse(a.equals(c));
    }

    @Test
    public void testWBar() {
        Task myTask = new Task("a", "", 1.0);

        assertThat(1.0,
                is(HeftPlanner.meanComputationTime(myTask, makeUniformVMS(3))));

        assertThat((2.0 + 4 + 1)/3,
                is(HeftPlanner.meanComputationTime(myTask, makeNonUniformVMS())));

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

    @Test
    public void testRankU() {

        DAG tasks = makeTasks();
        Map<VMType, Integer> vms = makeUniformVMS(3);

        // Check that the final tasks only depend on themselves
        assertThat(HeftPlanner.upwardRank(tasks.getTaskById("f"), vms),
                is(HeftPlanner.meanComputationTime(tasks.getTaskById("f"), vms)));


        // Check for one task deeper into the dag (assuming that f and g
        // cost the same).
        double expectedRank = HeftPlanner.meanComputationTime(tasks.getTaskById("e"), vms)
                // + HeftPlanner.cBar(tasks.getTaskById("e"), tasks.getTaskById("g"))
                + HeftPlanner.upwardRank(tasks.getTaskById("g"), vms);

        assertThat(HeftPlanner.upwardRank(tasks.getTaskById("e"), vms), is(expectedRank));
    }

    @Test
    public void testRankedTasks() {
        DAG dag = makeTasks();

        // The correct result
        List<Task> expected = new ArrayList<Task>();
        expected.add(dag.getTaskById("b"));
        expected.add(dag.getTaskById("a"));
        expected.add(dag.getTaskById("d"));
        expected.add(dag.getTaskById("c"));
        expected.add(dag.getTaskById("e"));
        expected.add(dag.getTaskById("g"));
        expected.add(dag.getTaskById("f"));

        List<Task> actual = HeftPlanner.rankedTasks(dag, makeUniformVMS(3));
        assertThat(actual, is(expected));
    }

    @Test
    public void testCreatePlan() throws NoFeasiblePlan {

        Map<VMType, Integer> vmNumbers = makeUniformVMS(3);
        VMType vmt = vmNumbers.keySet().iterator().next(); // only one VMType

        DAG dag = makeTasks();
        List<Task> ranked = HeftPlanner.rankedTasks(dag, vmNumbers);

        Plan actual = HeftPlanner.createPlan(ranked, vmNumbers);

        Plan expected = new Plan();

        Resource r = new Resource(vmt);
        expected.schedule(r, dag.getTaskById("b"), 0.0);
        expected.schedule(r, dag.getTaskById("d"), 1.1);
        expected.schedule(r, dag.getTaskById("e"), 2.2);
        expected.schedule(r, dag.getTaskById("g"), 3.2);

        Resource r2 = new Resource(vmt);
        expected.schedule(r2, dag.getTaskById("a"), 0.0);
        expected.schedule(r2, dag.getTaskById("c"), 1.0);
        expected.schedule(r2, dag.getTaskById("f"), 3.2);

        // Resource with no jobs
        expected.resources.add(new Resource(vmt));

        assertSamePlans(actual, expected);
    }

    public void assertSamePlans(Plan actual, Plan expected) {
        assertThat(simplifyPlan(actual), is(simplifyPlan(expected)));
    }

    //??ds testPlanWithFillIn



    // Helper code to compare plans
    // ============================================================

    // Since we aren't allow to implement .equals for Task we have to copy
    // the plan information to a new object which does implement .equals,
    // then compare the new objects.

    // I'm going to ignore the fact that two tasks from different DAGs can
    // have the same id for now, because it seems to be impossible to work
    // around here (maybe Task should store a DAG id as well?).


    /** Convert a Plan to a simpler object for comparison purposes */
    public List<SimplePlanEntry> simplifyPlan(Plan plan) {

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


    public static class SimplePlanEntry {

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
