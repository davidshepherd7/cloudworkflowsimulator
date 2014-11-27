package cws.core.algorithms.heterogeneous;

import cws.core.dag.DAG;
import java.util.List;
import cws.core.core.VMType;
import cws.core.algorithms.Plan;

/**
 * Create a plan for the given dag using the VM types given.
 *
 * @author David Shepherd
 */
    public interface Planner {

    public abstract Plan planDAG(DAG dag, List<VMType> availableVMTypes);
}
