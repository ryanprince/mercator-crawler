import { sleep } from "../utils/GTOR";
import { URLFrontierSettings } from "./url-frontier";

export const backQueueSelector: URLFrontierSettings["backqueueSelector"] =
	async ({ backQueue, hostHeap, backQueueRouter }) => {
		while (true) {
			const host = await hostHeap.next();
			if (!host) return;
			const answer = await Promise.race([
				(await backQueueRouter({ backQueue, host }))?.dequeue(),
				sleep(0).then(() => null),
			]);
			if (answer) return answer;

			// The backqueue for the host was empty, indicating that no URLs
			// for the host remain to be processed at this time, so we delete
			// the host's entry from the priority heap.
			hostHeap.deleteHost(host);
		}
	};
