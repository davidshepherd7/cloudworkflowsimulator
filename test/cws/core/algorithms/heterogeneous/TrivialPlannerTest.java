package cws.core.algorithms.heterogeneous;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.HashSet;


import cws.core.dag.DAG;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.Task;
import cws.core.dag.algorithms.TopologicalOrder;

import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;

public class TrivialPlannerTest {

    @Test
    public void testCreateBasicPlan() throws NoFeasiblePlan {

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

        VMType fastvmtype = VMTypeBuilder.newBuilder().mips(20).cores(1).price(1).build();
        VMType slowvmtype = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1).build();
        List<VMType> vmtypes = Arrays.asList(slowvmtype, fastvmtype);

        Planner planner = new TrivialPlanner();
        Plan plan = planner.planDAG(dag, vmtypes);

        // All planned
        assertAllTasksArePlanned(plan, dag);

        // To one VM
        assertThat(plan.resources.size(), is(1));

        // Which is the fastest VM
        assertThat(plan.resources.iterator().next().vmtype, is(fastvmtype));


        // Order is handled by TopologicalOrder, so safe to assume it's ok
        // if that classes tests are passing.
    }


    private void assertAllTasksArePlanned(Plan plan, DAG dag) {

        Set<Task> planned = new HashSet<Task>();
        for(Resource r : plan.resources) {
            for(Slot s : r.getSlots()) {
                planned.add(s.task);
            }
        }

        // Pull out task list via ordering because it's much easier
        TopologicalOrder order = new TopologicalOrder(dag);
        Set<Task> allTasks = new HashSet<Task>();
        for(Task t : order) {
            allTasks.add(t);
        }

        // Comparing this way gives better failure output that
        // assert(a.equals(b)) etc.
        assertThat(planned, is(allTasks));
    }

}
