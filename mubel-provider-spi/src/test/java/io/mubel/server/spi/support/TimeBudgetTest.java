package io.mubel.server.spi.support;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TimeBudgetTest {

    @Test
    void returns_true_when_budget_is_not_exceeded() throws Exception {
        Duration duration = Duration.ofSeconds(1);
        var budget = new TimeBudget(duration);
        assertTrue(budget.hasTimeRemaining());
        Thread.sleep(100);
        assertTrue(budget.hasTimeRemaining());
        Thread.sleep(400);
        assertTrue(budget.hasTimeRemaining());
        Thread.sleep(500);
        assertFalse(budget.hasTimeRemaining());
    }

}