package cws.core.core.power;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import cws.core.core.power.PowerState;
import cws.core.core.power.PowerModel;
import cws.core.core.power.NoSuchPowerStateException;
import cws.core.core.power.StaticPowerModel;
import cws.core.core.power.CubicPowerModel;


public class PowerModelTest {

    @Test
    public void shouldGetCurrentStateStatic() {
        double power = 5.1;
        double mips = 2.5;
        PowerModel powerModel = new StaticPowerModel(power, mips);
        PowerState p = powerModel.getCurrentState();

        assertEquals(p.mips, mips, 0);
        assertEquals(p.power, power, 0);
    }

    @Test
    public void shouldGetCurrentStateCubic() {
        PowerModel powerModel = new CubicPowerModel(2.0, 0.5,
                                                    1.0, 3.5,
                                                    5, 0);
        PowerState p = powerModel.getCurrentState();

        assertEquals(p.mips, 1.0, 0);
        assertEquals(p.power, 2.0, 0);
    }

    @Test
    public void shouldIncrementDecrementStateCubic() {
        PowerModel powerModel = new CubicPowerModel(2.0, 0.5,
                                                    1.0, 3.5,
                                                    5, 0);
        PowerState p1 = powerModel.getCurrentState();

        powerModel.setHigherPowerState();
        powerModel.setLowerPowerState();
        PowerState p2 = powerModel.getCurrentState();

        assertEquals(p1, p2);
    }
}

// Other Things to test:

// Nonexistant power states return null if you look at them, throw if you
// try to access them. But might get rid of this...
