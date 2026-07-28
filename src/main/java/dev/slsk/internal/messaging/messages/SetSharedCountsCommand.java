// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;

/** Reports the number of shared directories and files. */
public final class SetSharedCountsCommand implements OutgoingMessage {
    private final int directoryCount;
    private final int fileCount;

    public SetSharedCountsCommand(int directoryCount, int fileCount) {
        this.directoryCount = directoryCount;
        this.fileCount = fileCount;
    }

    public int getDirectoryCount() {
        return directoryCount;
    }

    public int getFileCount() {
        return fileCount;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.SHARED_FOLDERS_AND_FILES)
                .writeInteger(directoryCount)
                .writeInteger(fileCount)
                .build();
    }
}
