package io.mubel.server.scheduling;

public final class ScheduledEventQueueNames {

    private ScheduledEventQueueNames() {
    }

    public static String deadlineQueueName(String esid) {
        return esid + "-dl";
    }

    public static String scheduleQueueName(String esid) {
        return esid + "-sc";
    }

}
