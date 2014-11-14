package cws.core.core.power;

import java.util.NoSuchElementException;

/**
 * Thrown when try to get a power state that doesn't exist.
 */
public class NoSuchPowerStateException extends NoSuchElementException {

    public NoSuchPowerStateException(String string) {
        super(string);
    }

    public NoSuchPowerStateException() {
        super("");
    }

    private static final long serialVersionUID = 1L;
}
