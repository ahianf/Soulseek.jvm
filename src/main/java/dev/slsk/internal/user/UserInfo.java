// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.user;

import dev.slsk.internal.messaging.messages.UserInfoResponseFactory;
import java.util.Arrays;

/** The response to a user-info request. */
public record UserInfo(String description, int uploadSlots, int queueLength, boolean freeUploadSlot, byte[] picture) {
    public UserInfo {
        picture = picture == null ? null : picture.clone();
    }

    /** Creates user information without a picture. */
    public UserInfo(String description, int uploadSlots, int queueLength, boolean freeUploadSlot) {
        this(description, uploadSlots, queueLength, freeUploadSlot, null);
    }

    /** Returns whether picture data was supplied. */
    public boolean hasPicture() {
        return picture != null;
    }

    /** Returns a defensive copy of the picture data. */
    @Override
    public byte[] picture() {
        return picture == null ? null : picture.clone();
    }

    /** Serializes this user-info response to its peer protocol message. */
    public byte[] toByteArray() {
        return UserInfoResponseFactory.toByteArray(this);
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || (other instanceof UserInfo that
                        && uploadSlots == that.uploadSlots
                        && queueLength == that.queueLength
                        && freeUploadSlot == that.freeUploadSlot
                        && java.util.Objects.equals(description, that.description)
                        && Arrays.equals(picture, that.picture));
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(description, uploadSlots, queueLength, freeUploadSlot);
        return 31 * result + Arrays.hashCode(picture);
    }
}
