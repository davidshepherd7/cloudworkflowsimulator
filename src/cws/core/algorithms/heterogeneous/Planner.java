package cws.core.algorithms.heterogeneous;

import cws.core.dag.DAG;
import java.util.Map;
import cws.core.core.VMType;
import cws.core.algorithms.Plan;
import cws.core.algorithms.Plan.NoFeasiblePlan;


/**
 * Create a plan for the given dag on top of the initial plan given (which
 * may contain some resources and some scheduled tasks).
 *
 * @author David Shepherd
 */
    public interface Planner {

        public abstract Plan planDAG(DAG dag, Plan currentPlan) throws NoFeasiblePlan;
    }
