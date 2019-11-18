package de.leue.carsten.rx.operators;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.functions.Action;
import io.reactivex.functions.Function;
import io.reactivex.subjects.PublishSubject;

public class Backpressure {

	/**
	 * Token that identifies the empty state
	 */
	private static final Object EMPTY = new Object();

	/**
	 * Token that identifies the idle state
	 */
	private static final Object IDLE = new Object();

	/**
	 * Adds an element to an array and returns that array
	 * 
	 * @param aObj - new item
	 * @param aDst - target array
	 * @return the target array for convenience
	 */
	private static final <T> ArrayList<T> arrayPush(final T aObj, final ArrayList<T> aDst) {
		aDst.add(aObj);
		return aDst;
	}

	/**
	 * Creates a transformer that transforms a source stream into a target stream
	 * using a mapper that can efficiently handle chunks of data
	 * 
	 * @param <T>     - type of the upstream elements
	 * @param <R>     - type of the downstream elements
	 * @param aMapper - the mapper than can handle chunks of data, efficiently
	 * 
	 * @return the resulting observable
	 * @see Observable#to(Function)
	 */
	public static final <T, R> Function<? super Observable<? extends T>, Flowable<R>> chunkedBackpressure(
			Function<? super Iterable<? extends T>, ? extends Publisher<R>> aMapper) {
		return (final Observable<? extends T> src$) -> createDeferredObservableSource(src$, aMapper);
	}

	/**
	 * The defer wrapper so we can safely keep function level state
	 * 
	 * @param aSrc$   - the source sequence
	 * @param aMapper - the mapper than can handle chunks of data, efficiently
	 * 
	 * @return the resulting observable
	 */
	private static final <T, R> Flowable<R> createDeferredObservableSource(Observable<? extends T> aSrc$,
			Function<? super Iterable<? extends T>, ? extends Publisher<R>> aMapper) {
		return Flowable.defer(() -> createObservableSource(aSrc$, aMapper));
	}

	/**
	 * Converts a source sequence into a target sequence using a chunk mapper
	 * 
	 * @param aSrc$   - the source sequence
	 * @param aMapper - the mapper than can handle chunks of data, efficiently
	 * 
	 * @return the resulting observable
	 */
	@SuppressWarnings("unchecked")
	private static final <T, R> Publisher<R> createObservableSource(final Observable<? extends T> aSrc$,
			final Function<? super Iterable<? extends T>, ? extends Publisher<R>> aMapper) {
		/**
		 * Flag to check if the source sequence is done. We need this to potentially
		 * flush the final buffer.
		 */
		final AtomicBoolean bDone = new AtomicBoolean(false);
		/**
		 * Source sequence setting the done flag when it completes
		 */
		final Observable<? extends T> obj$ = aSrc$.doFinally(() -> bDone.set(true));
		/**
		 * Triggers when the generated downstream sequence has terminated
		 */
		final PublishSubject<Object> idle$ = PublishSubject.create();

		// signal that downstream is idle
		final Action finalAction = () -> idle$.onNext(IDLE);

		// shortcut
		final Function<Object, ? extends Flowable<R>> toFlowable = Backpressure::toFlowable;

		/**
		 * We merge the original events and the events that tell about downstream
		 * readiness
		 */
		return Observable.merge(obj$, idle$)
				/**
				 * The accumulator is either `undefined`, a (non-empty) buffer or an
				 * Observable<R>
				 *
				 * - if `undefined` the downstreams is idle and we do not have a buffer, yet -
				 * if a buffer, downstream is busy and we already have a buffered item - if an
				 * observable, downstream is busy but there is no buffered item, yet
				 */
				.scan(EMPTY, (final Object acc, final Object obj) -> {
					/**
					 * The idle event indicates that downstream had been busy but is idle, now
					 */
					if (isIdle(obj)) {
						/**
						 * if there is data in the buffer, process downstream and reset the buffer
						 */
						if (isBuffer(acc)) {
							// process the next chunk of data
							return aMapper.apply((List<? extends T>) acc);
						}
						/**
						 * Check if the sequence is done
						 */
						if (bDone.get()) {
							/**
							 * nothing to process, but source is done. Also complete the backpressure stream
							 */
							idle$.onComplete();
						}
						// nothing to return
						return EMPTY;
					}
					// we have a buffer, append to it
					if (isBuffer(acc)) {
						return arrayPush((T) obj, (ArrayList<T>) acc);
					}
					// we have a running observable, start a new buffer
					if (isNotNil(acc)) {
						// downstream is busy, start buffering
						return newBuffer((T) obj);
					}
					// downstream is idle
					return aMapper.apply(singletonList((T) obj));
				})
				// only continue if we have new work
				.filter(Backpressure::isBusy)
				// convert to flowable without extra backpressure
				.toFlowable(BackpressureStrategy.ERROR)
				// construct an observable source
				.map(toFlowable)
				// append the resulting items and make sure we get notified about the readiness
				.concatMap(res$ -> res$.doFinally(finalAction));

	}

	/**
	 * Checks if the object is a buffer
	 * 
	 * @param aValue - value to check
	 * @return <code>true</code> if we have a buffer, else <code>false</code>
	 */
	private static final boolean isBuffer(final Object aValue) {
		return aValue instanceof ArrayList<?>;
	}

	/**
	 * Checks if we have some downstream handler running
	 * 
	 * @param aValue - value to check
	 * @return <code>true</code> if we have an observable source, else
	 *         <code>false</code>
	 */
	private static final boolean isBusy(final Object aValue) {
		return aValue instanceof Publisher<?>;
	}

	/**
	 * Tests if the object is the idle token
	 * 
	 * @param aValue - value to check
	 * @return <code>true</code> if we have the idle token, else <code>false</code>
	 */
	private static final boolean isIdle(final Object aValue) {
		return aValue == IDLE;
	}

	/**
	 * Checks if we have a non-empty object
	 * 
	 * @param aValue - value to check
	 * @return <code>true</code> if the value is not empty, else <code>false</code>
	 */
	private static final boolean isNotNil(final Object aValue) {
		return aValue != EMPTY;
	}

	/**
	 * Constructs a new list and adds a value to it
	 * 
	 * @param aValue - the value
	 * @return the new buffer
	 */
	private static final <T> ArrayList<T> newBuffer(final T aValue) {
		return arrayPush(aValue, new ArrayList<T>());
	}

	/**
	 * Converts an {@link ObservableSource} to an {@link Observable}.
	 * 
	 * @param aValue - the value to check
	 * @return the resulting {@link Observable}
	 */
	@SuppressWarnings("unchecked")
	private static final <R> Flowable<R> toFlowable(final Object aValue) {
		return Flowable.fromPublisher((Publisher<R>) aValue);
	}

}
