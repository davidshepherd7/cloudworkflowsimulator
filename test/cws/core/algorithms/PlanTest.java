package cws.core.algorithms;


import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;


import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.Collection;
import java.util.Iterator;

import static java.lang.Math.max;

import cws.core.core.VMType;
import cws.core.dag.Task;


import cws.core.VM;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.VMFactory;
import cws.core.provisioner.ConstantDistribution;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;
import cws.core.dag.algorithms.TopologicalOrder;


import cws.core.algorithms.Plan.Resource;
import cws.core.algorithms.Plan.Solution;
import cws.core.algorithms.Plan.Slot;
import cws.core.algorithms.Plan.NoFeasiblePlan;


public class PlanTest {

    public VMType makeVM() {
        return VMTypeBuilder.newBuilder().mips(1.0).
                cores(1).price(1.0)
                .provisioningTime(new ConstantDistribution(0.0))
                .deprovisioningTime(new ConstantDistribution(0.0))
                .build();
    }

    @Test
    public void testFindGapBlankSchedule() {
        Resource r = new Resource(makeVM());
        assertThat(r.findFirstGap(3.0, 0.0), is(0.0));
        assertThat(r.findOverlapingSlots(), empty());
    }

    @Test
    public void testFindGapTrivialSchedule() {
        Resource r = new Resource(makeVM());
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 0.0, 1.0));
        assertThat(r.findFirstGap(3.0, 0.0), is(1.0));
        assertThat(r.findOverlapingSlots(), empty());
    }

    @Test
    public void testFindGapScheduleWithGap() {
        Resource r = new Resource(makeVM());
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 0.0, 1.0));
        r.addToSchedule(new Slot(new Task("b", "", 1.0), 4.0, 1.0));
        assertThat(r.findFirstGap(2.0, 0.0), is(1.0));
        assertThat(r.findOverlapingSlots(), empty());
    }

    @Test
    public void testFindGapScheduleWithInitialGap() {
        Resource r = new Resource(makeVM());
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 5.0, 1.0));
        assertThat(r.findFirstGap(2.0, 0.0), is(0.0));
        assertThat(r.findOverlapingSlots(), empty());
    }

    @Test
    public void testFindOverLapTimes() {
        Resource r = new Resource(makeVM());
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 0.0, 1.0));

        Slot badSlot = new Slot(new Task("a", "", 1.0), 0.5, 1.0);
        r.addToSchedule(badSlot);

        assertThat(r.findOverlapingSlots(), containsInAnyOrder(badSlot));
    }

    @Test
    public void testNonzeroStartInitialGap() {
        Resource r = new Resource(makeVM());
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 5.0, 1.0));
        assertThat(r.findFirstGap(2.0, 1.0), is(1.0));
        assertThat(r.findOverlapingSlots(), empty());
    }

    @Test
    public void testNonzeroStartGap() {
        Resource r = new Resource(makeVM());
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 3.0, 1.0));
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 5.0, 1.0));
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 7.0, 1.0));

        // Duration slightly less than one because floating point..
        assertThat(r.findFirstGap(0.99, 5), is(6.0));
        assertThat(r.findOverlapingSlots(), empty());
    }

    @Test
    public void testNonzeroStartAfterEnd() {
        Resource r = new Resource(makeVM());
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 3.0, 1.0));

        assertThat(r.findFirstGap(1, 5), is(5.0));
        assertThat(r.findOverlapingSlots(), empty());
    }

    @Test
    public void testNoGapWithNoTasks() {
        Resource r = new Resource(makeVM(), 0.0, 1.0);
        assertThat(r.findFirstGap(5, 0.0), nullValue());
    }

    @Test
    public void testNoGapWithOneTask() {
        Resource r = new Resource(makeVM(), 0.0, 6.0);
        r.addToSchedule(new Slot(new Task("a", "", 1.0), 3.0, 1.0));

        assertThat(r.findFirstGap(5, 0.0), nullValue());
    }

}
