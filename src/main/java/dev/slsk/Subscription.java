// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * A registered listener, and the means to unregister it.
 *
 * <p>Returning a handle rather than requiring the caller to hand the same
 * {@link java.util.function.Consumer} back to a {@code removeListener} method is
 * what makes a lambda usable as a listener at all. The old surface could not
 * unregister one, because the caller had no reference to compare.
 *
 * <p>{@link #close()} is idempotent and never throws, so it is safe in a
 * try-with-resources block and safe to call from a cleanup path that does not
 * know whether it already ran. It narrows {@link AutoCloseable#close()} to
 * remove the checked exception.
 */
public interface Subscription extends AutoCloseable {

    /**
     * Unregisters the listener.
     *
     * <p>Idempotent, and never throws. After this returns the listener will not
     * be invoked again for any event published subsequently. A publication
     * already in flight on another thread may still deliver.
     */
    @Override
    void close();
}
