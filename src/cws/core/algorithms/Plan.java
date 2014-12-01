package cws.core.algorithms;

import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.Collection;

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
        this.resources = new LinkedHashSet<Resource>(other.resources);
    }


    public double getCost() {
        double cost = 0.0;
        for (Resource r : resources) {
            cost += r.getCost();
        }
        return cost;
    }

    public String toString() {
        return resources.toString();
    }


    /** Easy way to add to schedule */
    public void schedule(Resource r, Task t, double start) {
        if(!resources.contains(r)) {
            resources.add(r);
        }

        final double duration = r.vmtype.getPredictedTaskRuntime(t);

        r.addToSchedule(new Slot(t, start, duration));
    }

    /** Get the time a task finishes in the schedule*/
    public double getFinishTime(Task t) {
        //??ds optimise?

        for(Resource r : resources) {
            for(Map.Entry<Double, Slot> entry : r.schedule.entrySet()) {
                Slot s = entry.getValue();
                if(s.task.equals(t)) {
                    return s.start + s.duration;
                }
            }
        }

        throw new RuntimeException("Task not in plan.");
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

        public String toString() {
            return String.format("Slot(%s, start: %f, duration: %f)",
                    task.toString(), start, duration);
        }
    }

    /** Class to group a vmtype (which will be created by the Algorithm
     * class when the plan is executed) with a schedule for the tasks
     * assigned to that VM.
     */
    public static class Resource {

        public VMType vmtype;

        private TreeMap<Double, Slot> schedule;

        public Resource(Resource other) {
            this(other.vmtype);
            this.schedule = new TreeMap<>(other.schedule);
        }

        public Resource(VMType vmtype) {
            this.vmtype = vmtype;
            this.schedule = new TreeMap<Double, Slot>();
        }

        public SortedSet<Double> getStartTimes() {
            return schedule.navigableKeySet();
        }

        /** Get an ordered collection of the slots in the schedule (in
         * order of start time).
         */
        public Collection<Slot> getSlots() {
            return schedule.values();
        }

        public void addToSchedule(Slot slot) {
            Slot previous = schedule.put(slot.start, slot);
            if(previous != null) {
                throw new RuntimeException("Tried to overwrite slot at time "
                        + Double.toString(previous.start));
            }
        }

        /** Find the time when the first gap of size desiredGapDuration is
         * available. Trys both before all slots have started and after all
         * slots have finished, as well as in between all scheduled slots.
         */
        public double findFirstGap(double desiredGapDuration) {

            // Try to fit it in before any of the slots start.
            if (schedule.size() == 0 || schedule.firstKey() > desiredGapDuration) {
                return 0.0;
            }

            // Try to fit in between all the other pairs of slots
            for (final Slot s : getSlots()) {
                final double finishTime = s.start + s.duration;

                // Get the start time of the next slot, null if there is no
                // next slot.
                final Double nextStart = schedule.higherKey(s.start);

                if(nextStart == null
                        || (nextStart - finishTime) > desiredGapDuration) {
                    return finishTime;
                }
            }

            // Never get here, should have found a slot with no following
            // slot in the above loop.
            throw new RuntimeException("Should never get here.");
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

        public String toString() {
            return "Resource(" + vmtype.toString() + ", " + schedule.toString() + ")";
        }
    }

    /** Class to group together a slot with it's cost and a Resource
     * for comparison purposes.
     */
    public static class Solution {
        final double cost;
        final Resource resource;
        final Slot slot;
        final boolean newresource;

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
            resource.addToSchedule(slot);
            p.resources.add(resource);
        }

        public String toString() {
            return String.format("Solution(%s, start = %f, duration = %f)",
                    resource.vmtype.toString(),
                    slot.start,
                    slot.duration);
        }
    }

    public static class NoFeasiblePlan extends Exception {
        private static final long serialVersionUID = 1L;

        public NoFeasiblePlan(String msg) {
            super(msg);
        }
    }

}
