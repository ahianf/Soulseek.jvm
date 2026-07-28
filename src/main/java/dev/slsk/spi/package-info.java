// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

/**
 * What you implement.
 *
 * <p>The other three exported packages are what you call, what you receive, and
 * what can go wrong. This one is the inversion: the points where the library
 * asks the application a question it cannot answer itself — where to put the
 * bytes of a download, what this account is sharing, what to do with a peer that
 * wants a file.
 *
 * <p>Every one of these is blocking, and every one has a working default. A
 * consumer that implements none of them still gets a correct client; a consumer
 * that implements one is not thereby signed up for a threading model.
 */
package dev.slsk.spi;
