package cws.core.core.power;

import cws.core.core.power.PowerState;
import cws.core.core.power.PowerModel;
import cws.core.core.power.NoSuchPowerStateException;

/**
 * A power model which only has one power state.
 *
 * @author David Shepherd
 */
public class StaticPowerModel implements PowerModel {

    PowerState powerState;

    public StaticPowerModel (double power, double mips) {
        powerState = new PowerState(power, mips);
    }

    @Override
    public PowerState getCurrentState() {
        return powerState;
    }

    @Override
    public PowerState getLowerPowerState() {
        return null;
    }

    @Override
    public PowerState getHigherPowerState() {
        return null;
    }

    @Override
    public void setHigherPowerState() throws NoSuchPowerStateException {
        throw new NoSuchPowerStateException();
    }

    @Override
    public void setLowerPowerState() throws NoSuchPowerStateException {
        throw new NoSuchPowerStateException();
    }

}
