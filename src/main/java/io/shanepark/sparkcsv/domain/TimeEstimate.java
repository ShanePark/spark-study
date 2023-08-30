package io.shanepark.sparkcsv.domain;

import java.util.concurrent.ConcurrentLinkedQueue;

public class TimeEstimate {

    // 1 byte / ms because it's really slow for the first request
    private final long INITIAL_FILE_PROCESS_PER_MILLISECOND = 1;

    // only use last 5 history
    private final int HISTORY_SIZE = 5;

    ConcurrentLinkedQueue<FileSizeAndTime> concurrentQueue = new ConcurrentLinkedQueue<>();

    public long calcEstimate(long fileSize) {
        if (concurrentQueue.isEmpty()) {
            return fileSize / INITIAL_FILE_PROCESS_PER_MILLISECOND;
        }

        long totalTime = 0;
        long totalFileSize = 0;

        for (FileSizeAndTime fileSizeAndTime : concurrentQueue) {
            totalTime += fileSizeAndTime.timeTaken;
            totalFileSize += fileSizeAndTime.fileSize;
        }
        return (long) (totalTime / (double) totalFileSize * fileSize);
    }

    public void addHistory(long fileSize, long timeTaken) {
        concurrentQueue.add(new FileSizeAndTime(fileSize, timeTaken));

        if (concurrentQueue.size() > HISTORY_SIZE) {
            concurrentQueue.poll();
        }
    }

    class FileSizeAndTime {
        long fileSize;
        long timeTaken;

        public FileSizeAndTime(long fileSize, long timeTaken) {
            this.fileSize = fileSize;
            this.timeTaken = timeTaken;
        }
    }

}
