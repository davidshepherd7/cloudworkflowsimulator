package cws.core.algorithms;

import java.util.Set;
import java.util.LinkedHashSet;
import java.util.SortedSet;
import java.util.TreeMap;

import cws.core.core.VMType;
import cws.core.dag.Task;


/**
 * A collection of struct-like classes to help with constructing plans for
 * static scheduling.
 */
public class Plan {
    public LinkedHashSet<Resource> resources;

    public Plan() {
        this.resources = new LinkedHashSet<Resource>();
    }

    public Plan(Plan other) {
        this.resources = new LinkedHashSet<Resource>();
        for (Resource r : other.resources) {
            this.resources.add(new Resource(r));
        }
    }
    

    public double getCost() {
        double cost = 0.0;
        for (Resource r : resources) {
            cost += r.getCost();
        }
        return cost;
    }

    /** Class to group together the start and duration of a Task in a
     * Plan.
     */
    public static class Slot {
        final public Task task;
        final public double start;
        final public double duration;

        public Slot(Task task, double start, double duration) {
            this.task = task;
            this.start = start;
            this.duration = duration;
        }
    }

    /** Class to group a vmtype (which will be created by the Algorithm
     * class when the plan is executed) with a schedule for the tasks
     * assigned to that VM.
     */
    public static class Resource {

        public VMType vmtype;

        public TreeMap<Double, Slot> schedule;

        public Resource(Resource other) {
            this(other.vmtype);
            for (Double s : other.schedule.navigableKeySet()) {
                schedule.put(s, other.schedule.get(s));
            }
        }

        public Resource(VMType vmtype) {
            this.vmtype = vmtype;
            this.schedule = new TreeMap<Double, Slot>();
        }

        public SortedSet<Double> getStartTimes() {
            return schedule.navigableKeySet();
        }

        public double getStart() {
            if (schedule.size() == 0) {
                return 0.0;
            }
            return schedule.firstKey();
        }

        public double getEnd() {
            if (schedule.size() == 0) {
                return 0.0;
            }
            double last = schedule.lastKey();
            Slot lastSlot = schedule.get(last);
            return last + lastSlot.duration + vmtype.getDeprovisioningDelayEstimation();
        }

        public int getFullBillingUnits() {
            return getFullBillingUnitsWith(getStart(), getEnd());
        }

        public int getFullBillingUnitsWith(double start, double end) {
            double seconds = end - start;
            double units = seconds / vmtype.getBillingTimeInSeconds();
            int rounded = (int) Math.ceil(units);
            return Math.max(1, rounded);
        }

        public double getCostWith(double start, double end) {
            return vmtype.getVMCostFor(end - start);
        }

        public double getCost() {
            return getCostWith(getStart(), getEnd());
        }

        public double getUtilization() {
            double runtime = 0.0;
            for (Slot sl : schedule.values()) {
                runtime += sl.duration;
            }
            return runtime / (getFullBillingUnits() * vmtype.getBillingTimeInSeconds());
        }
    }

    /** Class to group together a slot with it's cost and a Resource
     * for comparison purposes.
     */
    public static class Solution {
        double cost;
        Resource resource;
        Slot slot;
        boolean newresource;

        public Solution(Resource resource, Slot slot, double cost, boolean newresource) {
            this.resource = resource;
            this.slot = slot;
            this.cost = cost;
            this.newresource = newresource;
        }

        public boolean betterThan(Solution other) {
            // A solution is better than no solution
            if (other == null) {
                return true;
            }

            // Cheaper solutions are better
            if (this.cost < other.cost) {
                return true;
            }
            if (this.cost > other.cost) {
                return false;
            }

            // Existing resources are better
            if (!this.newresource && other.newresource) {
                return true;
            }
            if (this.newresource && !other.newresource) {
                return false;
            }

            // Earlier starts are better
            if (this.slot.start < other.slot.start) {
                return true;
            }
            if (this.slot.start > other.slot.start) {
                return false;
            }

            return true;
        }

        public void addToPlan(Plan p) {
            resource.schedule.put(slot.start, slot);
            p.resources.add(resource);
        }
    }

    public static class NoFeasiblePlan extends Exception {
        private static final long serialVersionUID = 1L;

        public NoFeasiblePlan(String msg) {
            super(msg);
        }
    }

}
