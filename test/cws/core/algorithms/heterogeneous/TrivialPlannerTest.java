package cws.core.algorithms.heterogeneous;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import static java.util.Arrays.asList;


import org.junit.Test;

import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.NoFeasiblePlan;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.Task;

public class TrivialPlannerTest  extends PlannerTestBase {

    @Test
    public void testCreateBasicPlan() throws NoFeasiblePlan {
        DAG dag = makeDiamondDAG();

        VMType fastvmtype = VMTypeBuilder.newBuilder().mips(20).cores(1).price(1).build();
        VMType slowvmtype = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1).build();
        List<VMType> vms = new ArrayList<>();
        vms.add(fastvmtype);
        vms.add(slowvmtype);

        Planner planner = new TrivialPlanner();
        Plan plan = planner.planDAG(dag, new Plan(vms));

        // All planned
        assertAllTasksArePlanned(plan, dag);

        Map<VMType, Resource> resourceMap = new HashMap<>();
        for (Resource r : plan.resources) {
            resourceMap.put(r.vmtype, r);
        }

        assertThat("The slow VM is ignored",
                resourceMap.get(slowvmtype).getSlots().size(),
                is(0));

        assertThat("The fast VM is used",
                resourceMap.get(fastvmtype).getSlots().size(),
                is(not(0)));

        // The order of the task execution is handled by TopologicalOrder,
        // so safe to assume it's ok if that classes tests are passing.
    }

    @Test
    public void testCreatePlanWithInitialPlan() throws NoFeasiblePlan {
        final DAG dag = makeDiamondDAG();

        final VMType fastvmtype = VMTypeBuilder.newBuilder().mips(20).cores(1).price(1).build();
        final Resource r = new Resource(fastvmtype);

        Task initialTask = new Task("initial", "", 1.0);
        Plan initialPlan = new Plan();
        initialPlan.resources.add(r);
        initialPlan.schedule(r, initialTask, 0.8);

        Planner planner = new TrivialPlanner();

        Plan plan = planner.planDAG(dag, initialPlan);

        // All dag tasks are planned
        assertAllTasksArePlanned(plan, dag);

        // The additional initial task is still planned
        assertAllTasksArePlanned(plan, asList(initialTask));
    }

}
