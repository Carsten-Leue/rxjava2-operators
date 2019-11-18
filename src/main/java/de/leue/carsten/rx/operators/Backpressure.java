package de.leue.carsten.rx.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Action;
import io.reactivex.functions.Function;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

/**
 * Collection of back-pressure operators
 * 
 * @author CarstenLeue
 *
 */
public class Backpressure {

	/**
	 * Simple pair holder
	 */
	private static class Pair {

		private boolean bDone = false;
		private Object data = EMPTY;
	}

	/**
	 * Token that identifies the done state
	 */
	private static final Object DONE = new Object();

	/**
	 * Token that identifies the empty state
	 */
	private static final Object EMPTY = new Object();

	/**
	 * Token that identifies the idle state
	 */
	private static final Object IDLE = new Object();

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
			Function<? super List<? extends T>, ? extends Publisher<R>> aMapper) {
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
			Function<? super List<? extends T>, ? extends Publisher<R>> aMapper) {
		// delegate
		final Function<? super List<? extends T>, Flowable<R>> mapper = lst -> Flowable
				.fromPublisher(aMapper.apply(Collections.unmodifiableList(lst)));
		// dispatch
		return Flowable.defer(() -> createObservableSource(aSrc$, mapper));
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
			final Function<? super List<? extends T>, Flowable<R>> aMapper) {
		/**
		 * Source sequence setting the done flag when it completes
		 */
		final Observable<Object> obj$ = ((Observable<Object>) aSrc$).concatWith(Single.just(DONE));
		/**
		 * Triggers when the generated downstream sequence has terminated
		 */
		final PublishSubject<Object> idle$ = PublishSubject.create();

		// signal that downstream is idle
		final Action finalAction = () -> idle$.onNext(IDLE);

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
				.scan(new Pair(), (final Pair pair, final Object obj) -> reducePair(pair, obj, aMapper, idle$))
				// extract the data
				.map(pair -> pair.data)
				// only continue if we have new work
				.filter(Backpressure::isBusy)
				// convert to flowable without extra backpressure
				.toFlowable(BackpressureStrategy.ERROR)
				// append the resulting items and make sure we get notified about the readiness
				.concatMap(res$ -> ((Flowable<R>) res$).doFinally(finalAction));

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
	 * Reducer implementation
	 * 
	 * @param <T>     type of the source sequence
	 * @param <R>     type of the target sequence
	 * @param pair    current state
	 * @param obj     input object, either the original object or the done indicator
	 * @param aMapper mapper callback
	 * @param aIdle$
	 * @return
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private static final <T, R> Pair reducePair(final Pair pair, final Object obj,
			final Function<? super List<? extends T>, Flowable<R>> aMapper, final Subject<Object> aIdle$)
			throws Exception {
		/**
		 * Extract some state
		 * 
		 */
		Object data = pair.data;
		final boolean bDone = pair.bDone;
		/**
		 * The idle event indicates that downstream had been busy but is idle, now
		 */
		if (obj == IDLE) {
			/**
			 * if there is data in the buffer, process downstream and reset the buffer
			 */
			if (isBuffer(data)) {
				/**
				 * Process the buffer
				 */
				data = aMapper.apply((ArrayList<T>) data);
			} else {
				/**
				 * Check if the sequence is done
				 */
				if (bDone) {
					/**
					 * nothing to process, but source is done. Also complete the backpressure stream
					 */
					aIdle$.onComplete();
				}
				// reset
				data = EMPTY;
			}
		} else
		/**
		 * The done event indicates that the source closed
		 */
		if (obj == DONE) {
			/**
			 * Set the done flag
			 */
			pair.bDone = true;
			/**
			 * if there is data in the buffer, process downstream and reset the buffer
			 */
			if (isBuffer(data)) {
				/**
				 * Process the buffer
				 */
				data = aMapper.apply((ArrayList<T>) data);
			} else {
				/**
				 * Check if the sequence is done
				 */
				if (data == EMPTY) {
					/**
					 * nothing to process, but source is done. Also complete the backpressure stream
					 */
					aIdle$.onComplete();
				}
				// reset
				data = EMPTY;
			}
		} else
		// we have a buffer, append to it
		if (isBuffer(data)) {
			// append to the buffer
			((ArrayList<T>) data).add((T) obj);
		} else
		// downstream is idle
		if (data == EMPTY) {
			// process the single item buffer
			data = aMapper.apply(Collections.singletonList((T) obj));
		} else {
			// we have a running observable, start a new buffer
			final ArrayList<T> buffer = new ArrayList<>();
			buffer.add((T) obj);
			// start a new chunk
			data = buffer;
		}
		// continue
		pair.data = data;
		return pair;
	}

	private Backpressure() {
	}

}
