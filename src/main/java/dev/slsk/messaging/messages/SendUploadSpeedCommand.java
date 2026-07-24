// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Reports the most recent upload speed. */
public final class SendUploadSpeedCommand extends IntegerServerMessage {
    public SendUploadSpeedCommand(int speed) {
        super(MessageCode.Server.SEND_UPLOAD_SPEED, speed);
    }

    public int getSpeed() {
        return value();
    }
}
