package util.retry;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.function.Predicate;

public class RetryServiceTest {
    private static final RetryService retryService = new RetryService();

    @Test
    public void testRetryServerWithPredicate() {
        Nexter nexter1 = Mockito.spy(new Nexter(4));
        Nexter nexter2 = Mockito.spy(new Nexter(5));
        Nexter nexter3 = Mockito.spy(new Nexter(10));
        Nexter nexter4 = Mockito.spy(new Nexter(4));
        Predicate<Boolean> predicate = Boolean::booleanValue;
        Optional<Boolean> result1 = retryService.retry(() -> Optional.of(nexter1.next()), new RetryPolicy(5, 0), predicate);
        Optional<Boolean> result2 = retryService.retry(() -> Optional.of(nexter2.next()), new RetryPolicy(5, 0), predicate);
        Optional<Boolean> result3 = retryService.retry(() -> Optional.of(nexter3.next()), new RetryPolicy(5, 0), predicate);
        Optional<Boolean> result4 = retryService.retry(
                () -> {
                    nexter4.next();
                    return Optional.empty();
                },
                new RetryPolicy(5, 0),
                predicate
        );
        Assert.assertTrue(result1.orElse(false));
        Assert.assertTrue(result2.orElse(false));
        Assert.assertFalse(result3.isPresent());
        Assert.assertFalse(result4.isPresent());
        Mockito.verify(nexter1, Mockito.times(4)).next();
        Mockito.verify(nexter2, Mockito.times(5)).next();
        Mockito.verify(nexter3, Mockito.times(5)).next();
        Mockito.verify(nexter4, Mockito.times(5)).next();
    }

    private class Nexter {
        private int current;
        private int target;

        Nexter(int target) {
            this.target = target;
            this.current = 0;
        }

        boolean next() {
            return ++current >= target;
        }
    }
}