package cws.core.algorithms.heterogeneous;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;



import org.junit.Test;

import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.NoFeasiblePlan;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;

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

        // To one VM
        assertThat(plan.resources.size(), is(1));

        // Which is the fastest VM
        assertThat(plan.resources.iterator().next().vmtype, is(fastvmtype));

        // The order of the task execution is handled by TopologicalOrder,
        // so safe to assume it's ok if that classes tests are passing.
    }


}
