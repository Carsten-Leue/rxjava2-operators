package de.leue.carsten;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import de.leue.carsten.rx.operators.Backpressure;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.functions.Function;
import io.reactivex.marble.junit.MarbleRule;

public class BackpressureTest {

	@Rule
	public MarbleRule marble = new MarbleRule();

	@Test
	public void shouldShowBackpressure() {

		final Flowable<String> down$ = MarbleRule.cold("---b|").toFlowable(BackpressureStrategy.BUFFER);

		final Observable<String> src$ = MarbleRule.cold("aa---aaaaaa-a-a-a-a-aaa------a|");

		final Flowable<String> back$ = src$
				.to(Backpressure.chunkedBackpressure((final Iterable<? extends String> buffer) -> down$));

		MarbleRule.expectFlowable(back$).toBe("---b---b---b---b---b---b---b----b|");
	}

	@Test
	public void shouldWorkWithSimpleBackpressure() {

		final Observable<Long> src$ = Observable.interval(100, TimeUnit.MILLISECONDS).delay(1000, TimeUnit.MILLISECONDS)
				.take(20);

		final Function<Iterable<? extends Long>, Flowable<Long>> handler = (final Iterable<? extends Long> buf) -> {
			System.out.println("buffer: " + buf);
			return Flowable.timer(500, TimeUnit.MILLISECONDS);
		};

		final Flowable<Long> evt$ = src$.to(Backpressure.chunkedBackpressure(handler));

		evt$.ignoreElements().blockingAwait();
	}
}
