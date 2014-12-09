package cws.core.algorithms.heterogeneous;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.Rule;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collections;
import java.util.NoSuchElementException;



public class PiecewiseConstantFunctionTest  {

    @Test
    public void testGetValueAtJumpTime() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction(0.0);
        p.addJump(0.0, 0.1);
        p.addJump(2.0, 2.0);

        assertThat(p.getValue(2.0), is(2.0));
    }

    @Test
    public void testGetValueBetweenJumpTimes() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction(0.0);
        p.addJump(0.0, 0.1);
        p.addJump(2.0, 2.0);

        assertThat(p.getValue(1.5), is(0.1));
    }

    @Test
    public void testGetValueAfterAnyTimes() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction(0.0);
        p.addJump(0.0, 0.1);
        p.addJump(2.0, 2.0);

        assertThat(p.getValue(2.5), is(2.0));
    }


    @Test
    public void testGetValueBeforeAnyTimes() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction(7.5);
        p.addJump(0.5, 0.1);
        p.addJump(2.0, 2.0);

        assertThat(p.getValue(0.0), is(7.5));
    }

    @Test
    public void testIntegralWithEndpointsAtJumps() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction(0.0);
        p.addJump(0.0, 0.1);
        p.addJump(2.0, 2.0);
        p.addJump(3.0, 3.0);


        assertThat(p.integral(0.0, 3.0), is(0.1*2.0 + 1.0*2.0));
    }

    @Test
    public void testIntegralWithEndpointsBetweenJumps() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction(0.0);
        p.addJump(0.0, 0.1);
        p.addJump(2.0, 2.0);
        p.addJump(3.0, 3.0);

        assertThat(p.integral(0.5, 2.5), is(0.1*1.5 + 2.0*0.5));
    }

    @Test
    public void testIntegralWithEndpointsAfterAllJumps() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction(0.0);
        p.addJump(0.0, 0.1);

        assertThat(p.integral(0.0, 2.0), is(0.1*2.0));
    }

    @Test
    public void testIntegralWithEndpointsBeforeAllJumps() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction(6.0);
        p.addJump(1.0, 0.5);

        assertThat(p.integral(-0.5, 2.0), is(6.0*1.5 + 1.0*0.5));
    }

}
