package util.retry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

public class RetryService {
    private static final Logger log = LogManager.getLogger();

    public <T> Optional<T> retry(Callable<Optional<T>> task) {
        return retry(task, value -> true);
    }

    public <T> Optional<T> retry(Callable<Optional<T>> task, Predicate<T> checker) {
        return retry(task, new RetryPolicy(20, 0), checker);
    }

    public <T> Optional<T> retry(Callable<Optional<T>> task, RetryPolicy retryPolicy) {
        return retry(task, retryPolicy, value -> true);
    }


    public <T> Optional<T> retry(Callable<Optional<T>> task, RetryPolicy retryPolicy, Predicate<T> checker) {
        log.debug("Starting retry service. Total number of attempts: {}.", retryPolicy.getTotalAttempts());
        while (retryPolicy.makeAttempt()) {
            log.debug("Starting attempt {} of {}.", retryPolicy.getCurrentAttempt(), retryPolicy.getTotalAttempts());
            try {
                Optional<T> result = task.call();
                if (result.isPresent() && checker.test(result.get())) {
                    log.debug("Successfully got result in {} attempts.", retryPolicy.getCurrentAttempt());
                    return result;
                }
                log.debug("Attempt {}. Failed to got result.", retryPolicy.getCurrentAttempt());
            } catch (Exception e) {
                log.error("Error while attempting to get result.", e);
            }
        }
        log.debug("Couldn't get result in {} attempts.", retryPolicy.getTotalAttempts());
        return Optional.empty();
    }
}
