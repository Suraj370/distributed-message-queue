package com.surajpanda.dmq.wal;

import com.surajpanda.dmq.message.Message;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Wal {

    private final Path file;

    public Wal(Path file) {
        this.file = file;
    }

    public void append(Message message) throws IOException {

        String record = String.format(
                "%s|%s|%s|%s|%d|%d%n",
                message.id(),
                message.key(),
                message.payload(),
                message.timestamp(),
                message.partition(),
                message.offset()
        );

        Files.writeString(
                file,
                record,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }
}