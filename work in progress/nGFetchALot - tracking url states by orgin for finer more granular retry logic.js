/**
 * Concurrently fetches a queue of Google API requests using a small pool of workers,
 * automatically grouping requests that share a `batchUrl` into Google API multipart/mixed
 * batch calls, following `nextPageToken` pagination to completion for every item, and
 * retrying failures using both a per-item retry budget and a global rate-limit circuit
 * breaker that pauses — and can ultimately halt — the entire pool.
 *
 * Each entry in `queue` is either a **solo request** (fetched directly)
 * or a **batch request** that is grouped with other items sharing the same `batchUrl`.
 *
 * Processing begins as soon as this function is called — it does not need to be awaited to
 * start. This function itself returns synchronously with a controller so the
 * caller can stop processing early. Await the returned `done` promise if you need to
 * know when the run has fully finished.
 *
 * @param {Object}         options                      - Configuration options for the run
 * @param {string}         options.authToken            - OAuth2 bearer token, sent as `Authorization: Bearer <token>` on every request
 * @param {Object}        [options.queue=[]           ] - The requests to process
 * @param {string}         options.queue[].id           - Unique identifier for this request, used in callbacks and error reporting
 * @param {string}        [options.queue[].url        ] - The full URL to fetch for a solo request
 * @param {string}        [options.queue[].batchUrl   ] - The base URL for a Google API batch request (e.g., `https://www.googleapis.com/batch/drive/v3`)
 * @param {string}        [options.queue[].apiPath    ] - The API path for a Google API batch request (e.g., `/drive/v3/files/abc`)
 * @param {string}        [options.queue[].contentType] - The `Content-Type` header for this request (default: `application/json`)
 * @param {Object|string} [options.queue[].body       ] - The request body for this request (default: none)
 * @param {Function}      [onItemDone                 ] - Called once per queue item, after its final page has been fetched, with every page
 *                                                        collected for that item.
 *                                                        signature: `(id: string, pages: Array<Object>) => void`
 * @param {Function}      [onItemNextPage             ] - Called each time a page is fetched for an item and the response indicated another page
 *                                                        remains (a `nextPageToken` was present).
 *                                                        signature: `(id: string, numPages: number) => void`
 * @param {Function}      [onQueueDone                ] - Called once, after every worker has exited because the queue is fully drained.
 *                                                        Not called if `stop()` was invoked or the circuit breaker tripped.
 *                                                        signature: `(fetchCount: number) => void`
 * @param {Function}      [onError                    ] - Called for every unrecoverable failure: per-item retry exhaustion,
 *                                                        non-retryable HTTP responses, unknown/network errors, and the global circuit
 *                                                        breaker tripping.
 *                                                        signature: `(details: Object) => void`
 * @param {number}        [maxWorkers=4               ] - Number of concurrent workers pulling from the queue.
 *                                                        Each worker processes one solo item or one batch group at a time.
 * @param {number}        [maxGlobalRetry=4           ] - Number of consecutive global rate-limit "waves" (429/5xx responses, deduplicated
 *                                                        across workers that hit the same wave simultaneously) tolerated before the
 *                                                        circuit breaker trips and all processing stops.
 * @param {number}        [maxItemRetry=4             ] - Number of times a single queue item may be retried before it is abandoned
 *                                                        and reported via `onError`.
 * @param {number}        [maxRetryDelay=30           ] - Upper bound, in seconds, on the exponential backoff delay used when a retryable
 *                                                        response doesn't include a usable `Retry-After` header.
 * @param {number}        [batchSize=50               ] - Maximum number of same-`batchUrl` items grouped into a single multipart batch request.
 * @param {string}        [batchBoundary="..."        ] - MIME multipart boundary string used when building batch request bodies.
 *                                                        Must not appear inside any request body it will wrap.
 *
 * @returns {Object}                                    - A controller for the run that was just started.
 *                                                        The `.stop()` method can be called to halt processing early, and the `.done`
 *                                                        promise resolves once all workers have exited (or the circuit breaker tripped).
 *
 * @throws {TypeError}                                  - If `authToken` is falsy, or if any of the call back functions are not functions.
 *
 * @example
 * const job = nGFetchALot({
 *     authToken: token,
 *     queue: [
 *         { id: 'sheet-1', url: 'https://sheets.googleapis.com/...' },
 *         { id: 'file-1', batchUrl: 'https://www.googleapis.com/batch/drive/v3', apiPath: '/drive/v3/files/abc' }
 *     ],
 *     onItemDone: (id, pages) => console.log(id, 'finished with', pages.length, 'pages'),
 *     onError: (data) => console.error('item failed:', data)
 * });
 *
 * stopButton.onclick = () => job.stop();
 * await job.done;
 */
function nGFetchALot({
    authToken,
    queue: userQueue = [],
    onItemDone: userOnItemDone = (id, pages) => console.log([id, pages]),
    onItemNextPage: userOnItemNextPage = (id, numPages) => console.log([id, numPages]),
    onQueueDone: userOnQueueDone = (fetchCount) => console.log(`queue done done with a total of ${fetchCount} fetches`),
    onEvent: onUserEvent = (type, where, eventData) => console.log([eventName, eventData]),
    maxWorkers: MAX_CONCURRENT_WORKERS = 4,
    maxGlobalRetry: MAX_RETRY_ATTEMPTS_GLOBAL = 4,
    maxItemRetry: MAX_RETRY_ATTEMPTS_ITEM = 4,
    maxRetryDelay: MAX_RETRY_DELAY = 30,
    batchSize: GOOGLE_API_BATCHING_SIZE = 50,
    batchBoundary: GOOGLE_API_BATCHING_BOUNDARY = "batch_nGFetchALot_request_boundary",
})
{
    // #region make user provided functions safe to call

    const onItemDone = makeUserFunctionSafeToCall(userOnItemDone, "onItemDone");
    const onItemNextPage = makeUserFunctionSafeToCall(userOnItemNextPage, "onItemNextPage");
    const onQueueDone = makeUserFunctionSafeToCall(userOnQueueDone, "onQueueDone");
    const onEvent = makeUserFunctionSafeToCall(onUserEvent, "onEvent");

    // #endregion

    // #region global variables

    // settings/constants
    const RETRYABLE_HTTP_STATUS_CODES = {429: true, 500: true, 502: true, 503: true, 504: true};
    const TEXT_ENCODER = new TextEncoder();

    // make a copy of the queue
    // store the queue in reverse order from the input
    // this way we can pop from the end to get the next item in the queue
    // and we can push to the end for retries so they get queued up faster (on the next pop)
    const queue = userQueue
        .map((item, index) =>
        {
            // we need to make sure each item has a url or batchUrl
            const urlToValidate = item.url || item.batchUrl;

            if(!urlToValidate)
            {
                throw new Error(`Missing url/batchUrl in queue item ${index}: ${JSON.stringify(item)}`);
            }

            if(item.batchUrl && !item.apiPath)
            {
                throw new Error(`Missing apiPath in batch queue item ${index}: ${JSON.stringify(item)}`);
            }

            try
            {
                new URL(urlToValidate);
            }
            catch
            {
                throw new Error(`Invalid URL in queue item ${index}: ${urlToValidate}`);
            }

            return {
                ...item,
                _pages: []
            };
        })
        .reverse();

    let isDoneProcessingQueue = false; // if we need to stop all workers, for whatever reason
    let userStopped = false;

    let numActiveWorkers = 0;          // track how many concurrent workers we have
    let fetchCount = 0;                // how many times we call fetch

    // Track the state of each unique URL to avoid duplicate fetches
    // { cooldownWaitUntil: 0, failureCount: 0 }
    const urlStates = {};

    // #endregion

    // #region main execution

    // if no authtoken, we can't do anything
    if(!authToken)
    {
        throw new TypeError("The 'authToken' parameter is required.");
    }

    // do the main magic
    const done = (async () =>
    {
        try
        {
            // create all the workers
            // and wait for them to be completed
            await Promise.all(
                Array.from(
                    {
                        length: MAX_CONCURRENT_WORKERS
                    },
                    (_, index) => worker(`worker${index + 1}`)
                )
            );

            onQueueDone(fetchCount);
        }
        catch(error)
        {
            isDoneProcessingQueue = true;

            throw error; // rethrow the error so the user can catch it if they want
        }
    })();

    // return a controller the user can use to stop the queue processing
    return {
        stop: () =>
        {
            if(userStopped) return flase; // already tripped, no need to trip again

            isDoneProcessingQueue = true;
            userStopped = true;

            return true; // successfully tripped the kill switch
        },
        done: done
    };

    // #endregion

    // #region worker stuff functions

    // Find which queue item is next based on it's cooldown time
    function findNextQueueItem(now)
    {
        let bestIndex = -1;
        let bestCooldownWaitUntil = Infinity;

        for(let i = queue.length - 1; i >= 0; i--)
        {
            const item = queue[i];
            const url = item.url || item.batchUrl;
            const state = getUrlState(url);

            if(state.dead) continue; // skip URLs that have hit their retry limit

            const cooldownWaitUntil = state.cooldownWaitUntil;

            if(cooldownWaitUntil <= now) // This item is ready to be processed immediately
            {
                return {
                    nextItemIndex: i,
                    nextItem: item,
                    cooldownWaitUntil: now
                };
            }
            else if(cooldownWaitUntil < bestCooldownWaitUntil)
            {
                bestCooldownWaitUntil = cooldownWaitUntil;
                bestIndex = i;
            }
        }

        return bestIndex === -1 ? null : {
            nextItemIndex: bestIndex,
            nextItem: queue[bestIndex],
            cooldownWaitUntil: bestCooldownWaitUntil
        }
    }

    // Each worker runs in a loop until the queue is empty and all workers are done, or until the kill switch is tripped.
    async function worker(workerID)
    {
        while(!isDoneProcessingQueue)
        {
            const now = Date.now();

            // system is genuinely finished when there is nothing left in the queue and none of the workers are active
            if(queue.length === 0 && numActiveWorkers === 0)
            {
                // stop processing this worker's while loop, and any other worker's while loop
                isDoneProcessingQueue = true;
                break;
            }

            // queue is temporarily empty, but active workers are still running.
            if(queue.length === 0)
            {
                // sleep a bit to give another worker time to add to the queue
                await sleep(100);
                continue;
            }

            // find the next queue item to process, based on the cooldown time of each unique URL
            const result = findNextQueueItem(now);

            if(result === null)
            {
                // Every remaining item belongs to a dead URL — report and purge them all
                while(queue.length > 0)
                {
                    const item = queue.pop();

                    onEvent(
                        "item abandoned",
                        {
                            ...item,
                            ...getUrlState(item.url || item.batchUrl),
                        },
                    );
                }

                continue; // loop will now see empty queue + no active workers → exit
            }

            const {nextItemIndex, nextItem, cooldownWaitUntil} = result;

            // if this queue item still needs to wait for its cooldown, sleep until it's ready
            if(cooldownWaitUntil > now)
            {
                // This item is still in cooldown — sleep until it's ready, with jitter
                await sleep(cooldownWaitUntil - Date.now() + Math.random() * 1000);

                // double check flag in case global limit was tripped during the cooldown sleep
                if(isDoneProcessingQueue) break;

                // re-evaluate the queue after sleeping, since other workers may have processed items in the meantime
                continue;
            }

            // // double check if queue is temporarily empty, but active workers are still running.
            // if(queue.length === 0)
            // {
            //     // sleep a bit to give another worker time to add to the queue
            //     await sleep(100);
            //     continue;
            // }

            // we are gonna start doing work work so track how many workers we have
            numActiveWorkers++;

            try
            {
                const requestsToProcess = [];

                // --- ATOMIC TRANSACTION ZONE (No 'await' allowed here!) ---
                // if the next item is a batch item, we need to get the rest of the batch items that match the same batchUrl
                if(nextItem.batchUrl)
                {
                    const batchTargetUrl = nextItem.batchUrl;

                    // go through the queue to find the matching batch requests to process, up to the max batch size
                    const remainingQueueItems = [];
                    for(let i = queue.length - 1; i >= 0; i--)
                    {
                        const item = queue[i];
                        if(item.hasOwnProperty("batchUrl") && item.batchUrl === batchTargetUrl && requestsToProcess.length < GOOGLE_API_BATCHING_SIZE)
                        {
                            // Worker takes physical ownership of this item completely right now
                            requestsToProcess.push(item);
                        }
                        else
                        {
                            remainingQueueItems.push(item);
                        }
                    }

                    // Instantly update the global queue before yielding control
                    queue.length = 0;
                    for(const item of remainingQueueItems.reverse()) queue.push(item);
                }
                else
                {
                    // for solor requests, we can just remove item off the queue and process it
                    requestsToProcess.push(queue.splice(nextItemIndex, 1)[0]); // remove the item from the queue
                }
                // --- END ATOMIC ZONE ---

                // Now it is perfectly safe to await, because the items are completely
                // wiped out from the global queue. No other worker can see them.
                if(requestsToProcess[0].batchUrl)
                {
                    await handleBatchRequest(matchingBatchRequests);
                }
                else if(matchingBatchRequests.length > 0)
                {
                    await handleSoloRequest(matchingBatchRequests[0]);
                }
            }
            finally
            {
                // call is done, so worker is done
                numActiveWorkers--;
            }
        }
    }

    // Processes a batch request and handles the response
    async function handleBatchRequest(batchRequests)
    {
        // make a batch request
        const batchBodyContent = createBatchRequestBody(batchRequests);

        // call it
        await fetchPayload(
            {
                id: "BATCH",
                url: batchRequests[0].batchUrl,
                contentType: `multipart/mixed; boundary=${GOOGLE_API_BATCHING_BOUNDARY}`,
                body: batchBodyContent
            },
            {
                onSuccess: (responseData, contentType) => { },
                onRetryable: () => { },
                onRateLimitExceeded: () => { },

                zonSuccess: (responseData, contentType) => { },
                zonRateLimit: (key, cooldownWaitUntil, failureCount) => { },
                zonError: (message, data) =>
                {
                    onEvent(
                        "error",
                        "batch request",
                        batchRequests,
                        message,
                        data
                    );
                },

                xonSuccess: (responseData, contentType) =>
                {
                    const boundaryMatch = contentType.match(/boundary=([^;]+)/i);
                    const boundary = boundaryMatch ? boundaryMatch[1].trim().replace(/['"]/g, "") : null;

                    const responseParts = parseBatchRequestResponse(responseData, boundary);

                    // match parsed json respones against the original batch requests
                    for(let i = 0, numBatchRequests = batchRequests.length; i < numBatchRequests; ++i)
                    {
                        const batchRequest = batchRequests[i];
                        const itemId = batchRequest.id;

                        const responseMatch = responseParts[itemId];

                        if(responseMatch)
                        {
                            const {httpResponseCode, httpResponseMessage, retryAfter, responseJSON} = responseMatch;

                            // if the individual response was good, we can process it
                            if(httpResponseCode == 200)
                            {
                                processPages(batchRequest, responseJSON);
                            }
                            else if(RETRYABLE_HTTP_STATUS_CODES.hasOwnProperty(httpResponseCode))
                            {
                                if(batchRequest._retryCount < MAX_RETRY_ATTEMPTS_ITEM)
                                {
                                    batchRequest._retryCount++;
                                    queue.push(batchRequest)
                                }
                                else
                                {
                                    onError({
                                        requestType: "batch item",
                                        request: batchRequest,
                                        reason: "retry limit exceeded",
                                        errorCode: httpResponseCode,
                                        errorMessage: httpResponseMessage,
                                        responseJSON: responseJSON,
                                        retrying: false,
                                    });
                                }
                            }
                            else
                            {
                                onError({
                                    requestType: "batch item",
                                    request: batchRequest,
                                    reason: "not retryable; skipping",
                                    errorCode: httpResponseCode,
                                    errorMessage: httpResponseMessage,
                                    responseJSON: responseJSON,
                                    retrying: false,
                                });
                            }
                        }
                        else
                        {
                            onError({
                                requestType: "batch item",
                                request: batchRequest,
                                reason: "not found in response",
                                response: responseData,
                                retrying: false,
                            });
                        }
                    }
                },
                xonRateLimit: (details) =>
                {
                    // TO DO onretry and onunknown do not increase retry count since the main batch request is what failed
                    onError({
                        ...details,
                        requestType: "batch container",
                        request: batchRequests,
                        retrying: true
                    });
                    // if the batch request was retryable
                    // push all the original requests back to the queue
                    // we have to reverse to keep priorities
                    // we also need to increase the retry count
                    let item;
                    while(item = batchRequests.pop())
                    {
                        if(item._retryCount < MAX_RETRY_ATTEMPTS_ITEM)
                        {
                            item._retryCount++;
                            queue.push(item);
                        }
                        else
                        {
                            onError({
                                requestType: "batch item",
                                request: item,
                                reason: "retry limit exceeded",
                                retrying: false
                            });
                        }
                    }
                },
                xonUnknown: (details) =>
                {
                    onError({
                        ...details,
                        requestType: "batch container",
                        request: batchRequests,
                        retrying: true
                    });

                    // if there was an unknown network error
                    // push all the original requests back to the queue
                    // we have to reverse to keep priorities
                    // we also need to increase the retry count
                    let item;
                    while(item = batchRequests.pop())
                    {
                        if(item._retryCount < MAX_RETRY_ATTEMPTS_ITEM)
                        {
                            item._retryCount++;
                            queue.push(item);
                        }
                        else
                        {
                            onError({
                                requestType: "batch item",
                                request: item,
                                reason: "retry limit exceeded",
                                retrying: false
                            });
                        }
                    }
                },
                xonError: (details) =>
                {
                    onError({
                        ...details,
                        requestType: "batch container",
                        request: batchRequests,
                        retrying: false
                    });
                }
            }
        );

    }

    // Processes a single solo request and handles the response
    async function handleSoloRequest(soloRequest)
    {
        await fetchPayload(
            soloRequest,
            {
                onSuccess: (responseData, contentType) => { },
                onRetryable: () => { },
                onRateLimitExceeded: () => { },

                zonSuccess: (responseData, contentType) => { },
                zonRateLimit: (key, cooldownWaitUntil, failureCount) => { },
                zonError: (message, data) =>
                {
                    onEvent(
                        "error",
                        "solo request",
                        soloRequest,
                        message,
                        data
                    );
                },

                xonSuccess: (responseData, contentType) =>
                {
                    processPages(soloRequest, responseData);
                },
                xonRateLimit: (details) =>
                {
                    onError({
                        ...details,
                        requestType: "solo",
                        request: soloRequest,
                        retrying: true
                    });

                    // retry the request if we can
                    if(soloRequest._retryCount < MAX_RETRY_ATTEMPTS_ITEM)
                    {
                        soloRequest._retryCount++;
                        queue.push(soloRequest);
                    }
                    else
                    {
                        onError({
                            requestType: "solo",
                            request: soloRequest,
                            reason: "retry limit exceeded",
                            retrying: false,
                        });
                    }
                },
                xonUnknown: (details) =>
                {
                    onError({
                        ...details,
                        requestType: "solo",
                        request: soloRequest,
                        retrying: true
                    });
                },
                xonError: (details) =>
                {
                    onError({
                        ...details,
                        requestType: "solo",
                        request: soloRequest,
                        retrying: false
                    });
                }
            }
        );
    }

    // Builds the raw multipart/mixed request body for a Google API batch call
    function createBatchRequestBody(batchRequests)
    {
        let batchBodyContent = "";

        batchBodyContent += batchRequests.map(batchRequest =>
        {
            // create the batch request for this request
            const hasBody = batchRequest.body !== undefined;
            const subMethod = hasBody ? 'POST' : 'GET'; // if the original request has a body, we need to change the batch request to post and
            const subBody = hasBody ? (typeof batchRequest.body === 'object' ? JSON.stringify(batchRequest.body) : batchRequest.body) : '';

            let partLines = [
                `--${GOOGLE_API_BATCHING_BOUNDARY}`,
                `Content-Type: application/http`,
                `Content-ID: <item-${batchRequest.id}>`,
                ``,                                               // Blank line separating multipart headers from the inner HTTP request
                `${subMethod} ${batchRequest.apiPath}`    // Inner HTTP Request Line
            ];

            // 2. Inner HTTP Headers (Must follow the Request Line directly)
            if(hasBody)
            {
                const byteLength = TEXT_ENCODER.encode(subBody).length;

                partLines.push(`Content-Type: ${batchRequest.contentType || 'application/json'}`);
                partLines.push(`Content-Length: ${byteLength}`);
                partLines.push(``); // Blank line separating inner headers from inner body
                partLines.push(subBody);
                partLines.push(``);
            }
            else
            {
                partLines.push(``);
                partLines.push(``);
            }

            // return this specific part's lines
            return partLines.join("\r\n");
        }).join(""); // join all the parts together

        // Close out the entire multipart transmission
        batchBodyContent += `--${GOOGLE_API_BATCHING_BOUNDARY}--\r\n`;

        return batchBodyContent;
    }

    // Parses a multipart/mixed batch response body into a lookup of per-item results
    function parseBatchRequestResponse(responseText, boundary)
    {
        const jsonResponses = {};

        // Normalize boundaries (Google prefixes them with -- in the body)
        const parts = responseText.split(`--${boundary}`);

        for(const part of parts)
        {
            const trimmedPart = part.trim();
            if(!trimmedPart || trimmedPart === "--") continue;

            // 1. Extract the item id from the Content-ID header.
            const itemIdMatch = trimmedPart.match(/Content-ID:\s*<response-item-(.*?)>/i) || trimmedPart.match(/Content-ID:\s*<item-(.*?)>/i);

            if(itemIdMatch)
            {
                const itemId = itemIdMatch[1].trim();

                // 2. Extract the inner HTTP status line (e.g., HTTP/1.1 200 OK)
                // const httpResponseParts = trimmedPart.match(/HTTP\/1\.1 ([0-9]+) (.*?)\r\n/i);
                const httpResponseParts = trimmedPart.match(/HTTP\/1\.1 ([0-9]+) (.*?)[\r\n]+/i);
                const retryAfter = trimmedPart.match(/Retry-After: (\d+)/i)?.[1] ?? null;

                if(!httpResponseParts)
                {
                    console.debug(`TO DO: batch response part has invalid HTTP status format | ${itemId}`);
                    continue;
                }

                // 3. Find the start of the inner HTTP response body
                // First, find where the inner HTTP payload actually starts by matching the status line
                const statusLineIndex = trimmedPart.search(/HTTP\/1\.1/i);
                if(statusLineIndex === -1)
                {
                    console.debug(`TO DO: Could not locate inner HTTP status payload | ${itemId}`);
                    continue;
                }

                // Isolate the entire inner HTTP block (Status Line + Inner Headers + JSON Body)
                const innerHttpPayload = trimmedPart.slice(statusLineIndex);

                // Now, find the double-newline sequence inside THIS payload to separate the inner headers from the JSON body
                let bodySeparatorIdx = innerHttpPayload.indexOf("\r\n\r\n");
                let separatorLength = 4;

                if(bodySeparatorIdx === -1)
                {
                    bodySeparatorIdx = innerHttpPayload.indexOf("\n\n");
                    separatorLength = 2;
                }

                if(bodySeparatorIdx === -1)
                {
                    console.debug(`TO DO: Inner HTTP headers and body are missing structural separator | ${itemId}`);
                    continue;
                }

                // Extract everything after the inner HTTP headers as the pure body string
                const jsonBodyString = innerHttpPayload.slice(bodySeparatorIdx + separatorLength).trim();

                if(!jsonBodyString)
                {
                    console.debug(`TO DO: Inner HTTP response body is empty | ${itemId}`);
                    continue;
                }

                // 4. Try parsing the clean JSON body
                try
                {
                    jsonResponses[itemId] = {
                        id: itemId,
                        httpResponseCode: httpResponseParts[1].trim(),
                        httpResponseMessage: httpResponseParts[2].trim(),
                        retryAfter: retryAfter, // TO DO: update logic to use retryAfter for batch item retries
                        responseJSON: JSON.parse(jsonBodyString), // Safely parses ONLY the pure JSON object
                    };
                }
                catch(e)
                {
                    console.debug(`TO DO: can't convert batch item response to json | ${itemId}. Error: ${e.message}`);
                    continue;
                }
            }
            else
            {
                console.debug("TO DO: batch item response missing Content-ID");
                continue;
            }
        }
        return jsonResponses;
    }

    // Processes a single page and queues the next page if it exists
    function processPages(queueItem, data)
    {
        // save the page
        queueItem._pages.push(data);
        queueItem._retryCount = 0;

        // if we have another page
        if(data.nextPageToken)
        {
            if(userStopped) return; // If the kill switch is tripped, do not queue more pages

            onItemNextPage(queueItem.id, queueItem._pages.length);

            // update the url with the next page token
            if(queueItem.url)
            {
                queueItem.url = setOrAppendURLQueryParameter(queueItem.url, 'pageToken', data.nextPageToken);
            }
            else if(queueItem.apiPath)
            {
                queueItem.apiPath = setOrAppendURLQueryParameter(queueItem.apiPath, 'pageToken', data.nextPageToken);
            }

            // requeue it
            queue.push(queueItem);
        }
        else
        {
            // no more pages, so return to user
            onItemDone(queueItem.id, queueItem._pages);
        }
    }

    // Uses exponential backoff to calculate a new global cooldown window after a retryable HTTP response.
    function handleRetryLogic(url, retryAfterOverride, onRetryable, onRateLimitExceeded)
    {
        const urlState = getUrlState(url);

        const now = Date.now();

        // If we are already in an active cooldown window, ignore subsequent concurrent errors
        // And call the retryable handler for handleBatchRequest/handleSoloRequest to handle the retry logic
        if(now < urlState.cooldownWaitUntil) return onRetryable();

        // If a significant amount of time has passed since our last failure, reset our backoff memory
        if(now - urlState.cooldownWaitUntil > 10000) urlState.failureCount = 0;

        urlState.failureCount++;

        // if we've hit max attempts for this url, we need to mark it dead
        if(urlState.failureCount >= MAX_RETRY_ATTEMPTS_GLOBAL)
        {
            urlState.dead = true; // Mark this URL as dead to prevent further retries

            return onRateLimitExceeded();
        }
        // otherwise we can calculate the cooldown window and retry
        else
        {
            if(retryAfterOverride)
            {
                // Respect explicit headers (convert seconds to ms)
                const parsedSeconds = parseInt(retryAfterOverride, 10);
                if(!isNaN(parsedSeconds))
                {
                    urlState.cooldownWaitUntil = now + (parsedSeconds * 1000);
                }
                else
                {
                    // Try parsing as HTTP-date format (e.g., "Wed, 21 Oct 2026 07:28:00 GMT")
                    const parsedDate = Date.parse(retryAfterOverride);
                    if(!isNaN(parsedDate))
                    {
                        urlState.cooldownWaitUntil = parsedDate;
                    }
                    else
                    {
                        const baseDelay = 2000;
                        const exponentialDelay = baseDelay * Math.pow(2, urlState.failureCount - 1);
                        urlState.cooldownWaitUntil = now + Math.min(exponentialDelay, MAX_RETRY_DELAY * 1000);
                    }
                }
            }
            else
            {
                const baseDelay = 2000;
                const exponentialDelay = baseDelay * Math.pow(2, urlState.failureCount - 1);
                urlState.cooldownWaitUntil = now + Math.min(exponentialDelay, MAX_RETRY_DELAY * 1000);
            }

            return onRetryable();
        }
    }

    // #endregion

    // #region fetch logic

    /**
     * Performs the actual `fetch()` call for one request (solo or batch) and classifies the
     * outcome into exactly one of four handlers:
     *
     *  - `onSuccess`   - (responseData, contentType)
     *  - `onRateLimit` - a retryable HTTP status (429, 500, 502, 503, or 504)
     *                  - (key, cooldownWaitUntil, failureCount)
     *  - `onError`     - on these errors:
     *                    - ("unknown fetch network error", error)
     *                    - ("unknown fetch google error", fetchResponse)
     *                    - ("retry limit exceeded", url)
     */
    async function fetchPayload({id, url, contentType, body}, {onSuccess, onRetryable, onRateLimitExceeded})
    {
        fetchCount++;

        // Dynamically build Fetch configuration options
        const fetchOptions = {
            method: body !== undefined ? "POST" : "GET",
            headers: {Authorization: `Bearer ${authToken}`},
        };

        // Dynamically apply Content-Type if provided, default to JSON for POSTs if omitted
        if(contentType) fetchOptions.headers["Content-Type"] = contentType;
        else if(body !== undefined) fetchOptions.headers["Content-Type"] = "application/json";

        // Attach body payload if present (stringifying if it's an object)
        if(body !== undefined) fetchOptions.body = typeof body === "object" ? JSON.stringify(body) : body;

        let fetchResponse;
        const urlState = getUrlState(url);

        // fetch in try to catch network errors
        try
        {
            fetchResponse = await fetch(url, fetchOptions);
        }
        catch(error)
        {
            // TO DO: catch transient network errors like a momentary disconnect; BUG-02 (https://gemini.google.com/app/d29994b23668ac56)

            // for now, just treat it as a retryable error and let the retry logic handle it
            console.error(`unknown fetch network error for ${url}; retrying:`, error);
            return handleRetryLogic(url, null, onRetryable, onRateLimitExceeded);
        }

        // Even though the request was good, Google/API returned an error code
        if(!fetchResponse.ok)
        {
            // if the request is retryable
            if(RETRYABLE_HTTP_STATUS_CODES.hasOwnProperty(fetchResponse.status))
            {
                return handleRetryLogic(url, fetchResponse.headers.get('Retry-After'), onRetryable, onRateLimitExceeded);
            }
            else
            {
                console.error(`unknown fetch google error for ${url}; retrying:`, fetchResponse);
                return handleRetryLogic(url, null, onRetryable, onRateLimitExceeded);
            }
        }
        else
        {
            // SUCCESS BRANCH: The request cleared perfectly!
            // Damping down the failure metrics incrementally instead of instantly zeroing them out.
            // This stops a "flapping" API from tricking the system into an infinite loop.
            if(urlState.failureCount > 0)
            {
                urlState.failureCount--;
            }

            const reponseContentType = fetchResponse.headers.get('content-type') || '';

            // batch requests will be multipart/mixed
            if(reponseContentType.includes('multipart/mixed'))
            {
                // get the text/body response
                const fetchResponseText = await fetchResponse.text();
                return onSuccess(fetchResponseText, reponseContentType);
            }
            else
            {
                const fetchResponseJson = await fetchResponse.json();
                return onSuccess(fetchResponseJson, reponseContentType);
            }
        }
    }


    // #endregion

    // #region helper functions

    // Wraps a user-supplied functions so they can be called safely without worrying about exceptions bubbling up and breaking the queue processing.
    function makeUserFunctionSafeToCall(userFunction, functionName)
    {
        if(typeof userFunction !== "function")
        {
            throw new TypeError(`The '${functionName}' parameter must be a function.`);
        }

        return (...args) =>
        {
            // no point in calling this function if the kill switch has been tripped because execution has stopped
            if(!userStopped)
            {
                try
                {
                    userFunction(...args);
                }
                catch(error)
                {
                    throw new Error(`Error in user-provided function '${functionName}':`, error);
                }
            }
        }
    }

    // Returns a promise that resolves after a fixed delay.
    function sleep(ms)
    {
        console.info(`Sleeping for ${ms} milliseconds...`);
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    // Sets or replaces a URL query parameter
    function setOrAppendURLQueryParameter(url, key, value)
    {
        const k = encodeURIComponent(key), v = encodeURIComponent(value);
        const pattern = new RegExp(`([?&]${k}=)[^&]*`);
        if(pattern.test(url)) return url.replace(pattern, `$1${v}`);
        return `${url}${url.includes('?') ? '&' : '?'}${k}=${v}`;
    }

    function getUrlKey(url)
    {
        // we don't need try/catch here since we did it when we cloned the user queue at the start
        return new URL(url).origin;
    }

    // get the state for a URL so we can track states and if we are in a cooldown or not for that URL
    function getUrlState(url)
    {
        // Use the origin (scheme + host + port) as the key
        let key = getUrlKey(url);

        if(!urlStates[key])
        {
            urlStates[key] = {
                cooldownWaitUntil: 0,
                failureCount: 0,
                dead: false
            };
        }
        return urlStates[key];
    }

    // #endregion
}
