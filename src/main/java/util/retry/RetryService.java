package util.retry;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

public class RetryService {

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
        while (retryPolicy.makeAttempt()) {
            try {
                Optional<T> result = task.call();
                if (result.isPresent() && checker.test(result.get())) {
                    return result;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return Optional.empty();
    }
}
