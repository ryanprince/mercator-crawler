import { URL as WHATWGURL } from "url";

import { sleep } from "../utils/GTOR";
import { Host, Link } from "./url-frontier";

export type EpochTimeStamp = number;

export class HostHeap {
	/** Hosts are mapped to their last-fetched timestamp, in order to implement politeness. */
	#map = new Map<Host, EpochTimeStamp>([]);
	timeBetweenHostFetches = 2000;
	sleepTime = 50;

	/** Adds the URL's host. If the last-fetched timestamp isn't given, then a default
	 * is computed to imply no pause -- useful for initializing the host's entry. */
	insertURL(
		url: Link,
		time: EpochTimeStamp = this.#timestampWithNoImpliedPause()
	) {
		this.insertHost(new WHATWGURL(url).host, time);
	}

	/** Upserts the host into the heap. If the host is already in the heap,
	 * then the larger of the stored and the input timestamps is used. */
	insertHost(
		host: Host,
		time: EpochTimeStamp = this.#timestampWithNoImpliedPause()
	) {
		const _time = this.#map.get(host) ?? time;
		this.#map.set(host, Math.max(_time, time));
	}

	/** Computes a timestamp that does not imply a delay to process; however,
	 * the host entry with this timestamp still might not process right away if
	 * there are other entries to be processed before it. */
	#timestampWithNoImpliedPause(): EpochTimeStamp {
		return Date.now() - this.timeBetweenHostFetches;
	}

	/** Removes the host from the heap. */
	deleteHost(host: Host) {
		this.#map.delete(host);
	}

	/** Returns a boolean indicating whether the heap contains an entry for the host. */
	has(host: Host): Boolean {
		return this.#map.has(host);
	}

	async next(): Promise<Host | undefined> {
		await sleep(0);

		// Find the entry that's scheduled to be processed next. This step
		// has some extra overhead and sub-optimal time complexity, due to
		// the use of a Map rather than a conventional heap data structure.
		const minTime = Math.min(...this.#map.values());
		if (minTime === Infinity) {
			return undefined;
		}
		const entries = [...this.#map.entries()];
		const nextEntry = entries.find(([_, time]) => time === minTime);
		const [host, time] = nextEntry ?? ["", 0];

		// Remove the host from the heap since this thread will process it.
		this.deleteHost(host);

		// Wait until it's time to fetch from the host, to guarantee politeness.
		while (time >= Date.now() - this.timeBetweenHostFetches) {
			await sleep(this.sleepTime);
		}

		// Add the host back to the heap with an updated timestamp for the
		// subsequent fetch's politeness.
		this.insertHost(host, Date.now());

		// Return the host to the caller for processing now that it's time
		// to process it.
		return host as Host;
	}

	async return() {
		this.#map.clear();
		return { value: undefined, done: true };
	}

	async throw() {
		this.#map.clear();
		return { value: undefined, done: true };
	}

	[Symbol.asyncIterator]() {
		return this;
	}
}
