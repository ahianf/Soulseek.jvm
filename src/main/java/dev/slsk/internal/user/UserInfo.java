// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.user;

import dev.slsk.internal.messaging.messages.UserInfoResponseFactory;

/**
 * The response to a user-info request.
 */
public class UserInfo {
    private final String description;
    private final boolean freeUploadSlot;
    private final boolean picturePresent;
    private final byte[] picture;
    private final int queueLength;
    private final int uploadSlots;

    /**
     * Creates user information without a picture.
     *
     * @param description the peer's description
     * @param uploadSlots the configured upload-slot count
     * @param queueLength the current queue length
     * @param hasFreeUploadSlot whether an upload slot is free
     */
    public UserInfo(String description, int uploadSlots, int queueLength, boolean hasFreeUploadSlot) {
        this(description, uploadSlots, queueLength, hasFreeUploadSlot, null);
    }

    /**
     * Creates user information.
     *
     * @param description the peer's description
     * @param uploadSlots the configured upload-slot count
     * @param queueLength the current queue length
     * @param hasFreeUploadSlot whether an upload slot is free
     * @param picture the picture data, if configured
     */
    public UserInfo(String description, int uploadSlots, int queueLength, boolean hasFreeUploadSlot, byte[] picture) {
        this.description = description;
        this.picturePresent = picture != null;
        this.picture = picture == null ? null : picture.clone();
        this.uploadSlots = uploadSlots;
        this.queueLength = queueLength;
        this.freeUploadSlot = hasFreeUploadSlot;
    }

    /**
     * Returns the user's description.
     *
     * @return the description
     */
    public final String getDescription() {
        return description;
    }

    /**
     * Returns whether an upload slot is free.
     *
     * @return whether an upload slot is free
     */
    public final boolean hasFreeUploadSlot() {
        return freeUploadSlot;
    }

    /**
     * Returns whether picture data was supplied.
     *
     * @return whether picture data was supplied
     */
    public final boolean hasPicture() {
        return picturePresent;
    }

    /**
     * Returns the picture data, if configured.
     *
     * @return a copy of the picture array, or {@code null}
     */
    public final byte[] getPicture() {
        return picture == null ? null : picture.clone();
    }

    /**
     * Returns the current queue length.
     *
     * @return the queue length
     */
    public final int getQueueLength() {
        return queueLength;
    }

    /**
     * Returns the configured upload-slot count.
     *
     * @return the upload-slot count
     */
    public final int getUploadSlots() {
        return uploadSlots;
    }

    /**
     * Serializes this user-info response to its peer protocol message.
     *
     * @return the framed response bytes
     */
    public byte[] toByteArray() {
        return UserInfoResponseFactory.toByteArray(this);
    }
}
