/** This is not as of yet a full mercator crawler.
 */

import { scrapeMeta } from "./metadata-fetcher";
import { Link, URLFrontier } from "./url-frontier/url-frontier";
import { AsyncQueue } from "./utils/AsyncQueue";
import { DePromisify } from "./utils/types";

export type DataFetcher<T> = (string: Link) => Promise<T>;

export type MercatorSettings<T> = {
	urlFrontier: URLFrontier;
	dataFetcher: DataFetcher<T>;
};

export type MercatorSettingsOverrides<T> = {
	dataFetcher: DataFetcher<T>;
}

export type DefaultFetcherReturn = DePromisify<ReturnType<typeof scrapeMeta>>;
const defaultMercatorSettings: MercatorSettings<DefaultFetcherReturn> =
	{
		urlFrontier: new URLFrontier(),
		dataFetcher: scrapeMeta,
	} as const;

export class Mercator<U> {
	#settings: MercatorSettings<U>;
	#inFrontierCache: Map<string, Promise<U>> = new Map();
	#fetchingData: Map<string, Promise<U>> = new Map();
	constructor(settings?: MercatorSettingsOverrides<U>) {
		this.#settings = {
			...defaultMercatorSettings,
			...settings
		} as MercatorSettings<U>;
	}

	async sendURL(url: Link) {
		const result = this.seedURL(url);
		await Promise.race([result, this.runToCompletion()]);
		return result;
	}

	async seedURL(url: string) {
		if (!this.#inFrontierCache.has(url)) {
			const result = this.#settings.urlFrontier
				.seedURL(url)
				.then(({ url }) => {
					return this.#getData(url);
				})
				.then((x) => {
					this.#inFrontierCache.delete(url);
					return x;
				});
			this.#inFrontierCache.set(url, result);
		}
		return (
			this.#inFrontierCache.get(url) ??
			Promise.reject(new Error("This should never be reached..."))
		);
	}

	async runToCompletion() {
		for await (const item of this) {
		}
	}

	async *[Symbol.asyncIterator]() {
		yield* this.#fetchURLs();
	}

	async #getData(url: string) {
		if (!this.#fetchingData.has(url)) {
			this.#fetchingData.set(
				url,
				this.#settings.dataFetcher(url).then((x) => {
					this.#fetchingData.delete(url);
					return x;
				})
			);
		}
		return (
			this.#fetchingData.get(url) ??
			Promise.reject(new Error("This should never be reached..."))
		);
	}

	async *#fetchURLs() {
		for await (const url of this.#settings.urlFrontier) {
			yield this.#getData(url);
		}
	}
}
