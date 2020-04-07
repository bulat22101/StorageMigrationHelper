package util.retry;

public class RetryPolicy {
    private int totalAttempts;
    private int currentAttempt;
    private long timeoutMS;

    public RetryPolicy(int totalAttempts, long timeoutMS) {
        this.totalAttempts = totalAttempts;
        this.currentAttempt = 0;
        this.timeoutMS = timeoutMS;
    }

    public boolean makeAttempt() {
        ++currentAttempt;
        if (timeoutMS > 0 && currentAttempt > 1) {
            try {
                Thread.sleep(timeoutMS);
            } catch (Exception ignored) {
            }
        }
        return currentAttempt <= totalAttempts;
    }

    public int getCurrentAttempt() {
        return currentAttempt;
    }

    public int getTotalAttempts() {
        return totalAttempts;
    }
}
