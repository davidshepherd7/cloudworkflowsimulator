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
        PiecewiseConstantFunction p = new PiecewiseConstantFunction();
        p.addJump(0.0, 0.1);
        p.addJump(2.0, 2.0);

        assertThat(p.getValue(2.0), is(2.0));
    }

    @Test
    public void testGetValueBetweenJumpTimes() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction();
        p.addJump(0.0, 0.1);
        p.addJump(2.0, 2.0);

        assertThat(p.getValue(1.5), is(0.1));
    }

    @Test
    public void testGetValueAfterAnyTimes() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction();
        p.addJump(0.0, 0.1);
        p.addJump(2.0, 2.0);

        assertThat(p.getValue(2.5), is(2.0));
    }


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGetValueBeforeAnyTimes() {
        PiecewiseConstantFunction p = new PiecewiseConstantFunction();
        p.addJump(0.5, 0.1);
        p.addJump(2.0, 2.0);

        thrown.expect(NoSuchElementException.class);
        p.getValue(0.0);
    }
    
}

                
