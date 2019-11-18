package de.leue.carsten;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import de.leue.carsten.rx.operators.Backpressure;
import io.reactivex.Observable;
import io.reactivex.functions.Function;
import io.reactivex.marble.junit.MarbleRule;

public class BackpressureTest {

	@Rule
	public MarbleRule marble = new MarbleRule();

	@Test
	public void shouldShowBackpressure() {

		final Observable<String> down$ = MarbleRule.cold("---b|");

		final Observable<String> src$ = MarbleRule.cold("aa---aaaaaa-a-a-a-a-aaa------a|");

		final Observable<String> back$ = src$
				.compose(Backpressure.chunkedBackpressure((final Iterable<? extends String> buffer) -> down$));

		MarbleRule.expectObservable(back$).toBe("---b---b---b---b---b---b---b----b|");
	}

	@Test
	public void shouldWorkWithSimpleBackpressure() {

		final Observable<Long> src$ = Observable.interval(100, TimeUnit.MILLISECONDS).delay(1000, TimeUnit.MILLISECONDS)
				.take(20);

		final Function<Iterable<? extends Long>, Observable<Long>> handler = (final Iterable<? extends Long> buf) -> {
			System.out.println("buffer: " + buf);
			return Observable.timer(500, TimeUnit.MILLISECONDS);
		};

		final Observable<Long> evt$ = src$.compose(Backpressure.chunkedBackpressure(handler));

		evt$.ignoreElements().blockingAwait();
	}
}
