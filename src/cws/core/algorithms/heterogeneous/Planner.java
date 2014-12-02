package cws.core.algorithms.heterogeneous;

import cws.core.dag.DAG;
import java.util.Map;
import cws.core.core.VMType;
import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.NoFeasiblePlan;


/**
 * Create a plan for the given dag using the VM types given.
 *
 * @author David Shepherd
 */
    public interface Planner {

        public abstract Plan planDAG(DAG dag, Map<VMType, Integer> vmNumbers) throws NoFeasiblePlan;
    }
