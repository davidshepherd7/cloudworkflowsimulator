package cws.core.core.power;

import java.util.List;

import cws.core.core.power.PowerState;
import cws.core.core.power.NoSuchPowerStateException;


/**
 * A class describing the power usage characteristics of a VM.
 */
public interface PowerModel {

    public PowerState getCurrentState();

    public PowerState getLowerPowerState();

    public PowerState getHigherPowerState();

    public void setHigherPowerState() throws NoSuchPowerStateException;

    public void setLowerPowerState() throws NoSuchPowerStateException;
}
