
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
 *                                                        signature: `(id: string, pages: Array<Object>, workerID: string) => void`
 * @param {Function}      [onItemNextPage             ] - Called each time a page is fetched for an item and the response indicated another page
 *                                                        remains (a `nextPageToken` was present).
 *                                                        signature: `(id: string, numPages: number, workerID: string) => void`
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
 * @param {boolean}       [debug=false                ] - When true, logs detailed per-iteration worker state via `console.debug`.
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
    onQueueDone: userOnQueueDone = () => console.log('queue done'),
    onError: userOnError = (data) => console.error(data),
    maxWorkers: MAX_CONCURRENT_WORKERS = 4,
    maxGlobalRetry: MAX_RETRY_ATTEMPTS_GLOBAL = 4,
    maxItemRetry: MAX_RETRY_ATTEMPTS_ITEM = 4,
    maxRetryDelay: MAX_RETRY_DELAY = 30,
    batchSize: GOOGLE_API_BATCHING_SIZE = 50,
    batchBoundary: GOOGLE_API_BATCHING_BOUNDARY = "batch_nGFetchALot_request_boundary",
    debug: DEBUG = false
})
{
    // #region make user provided functions safe to call

    const onItemDone = makeUserFunctionSafeToCall(userOnItemDone, "onItemDone");
    const onItemNextPage = makeUserFunctionSafeToCall(userOnItemNextPage, "onItemNextPage");
    const onQueueDone = makeUserFunctionSafeToCall(userOnQueueDone, "onQueueDone");
    const onError = makeUserFunctionSafeToCall(userOnError, "onError", true); // allow onError to be called even after the kill switch is tripped

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
        .map(item => ({
            ...item,
            _pages: [],
            _retryCount: 0
        }))
        .reverse();

    let isDoneProcessingQueue = false;            // if we need to stop all workers, for whatever reason
    let numActiveWorkers = 0;                     // track how many concurrent workers we have
    let fetchCount = 0;                           // how many times we call fetch

    // FIXED state management for BUG-04 and ARCH-01
    let globalCooldownWaitUntil = 0;              // Absolute time barrier workers must sleep until
    let consecutiveFailureCount = 0;              // Strict continuous failure wave counter (The Kill-Switch)
    let killSwitchTripped = false;

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

            if(!killSwitchTripped)
            {
                onQueueDone(fetchCount);
            }
        }
        catch(error)
        {
            killSwitchTripped = true;
            isDoneProcessingQueue = true;
            onError({
                message: "unexpected fatal error during queue processing",
                error
            });
        }
    })();

    // return a controller the user can use to stop the queue processing
    return {
        stop: () =>
        {
            if(killSwitchTripped) return flase; // already tripped, no need to trip again
            isDoneProcessingQueue = true;
            killSwitchTripped = true;

            return true; // successfully tripped the kill switch
        },
        done: done
    };

    // #endregion

    // #region worker stuff functions
    
    // Each worker runs in a loop until the queue is empty and all workers are done, or until the kill switch is tripped.
    async function worker(workerID)
    {
        var loopCounter = 0;
        while(!isDoneProcessingQueue)
        {
            loopCounter++;

            debugLog({
                where: "worker loop",
                workerID: workerID,
                loopCounter: loopCounter,
                queueLength: queue.length,
                numActiveWorkers: numActiveWorkers,
                fetchCount: fetchCount,
                globalCooldownWaitUntil: globalCooldownWaitUntil,
                consecutiveFailureCount: consecutiveFailureCount
            });

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

            // calculate how much time is left to wait for the cool down
            const baseWaitTime = globalCooldownWaitUntil - Date.now();
            // if we still have time left
            if(baseWaitTime > 0)
            {
                // add a worker specific jitter so all the workers don't wake at the same time
                await sleep(baseWaitTime + (Math.random() * 1000));

                // double check flag in case global limit was tripped during the cooldown sleep
                if(isDoneProcessingQueue) break;

                continue;
            }

            // double check if queue is temporarily empty, but active workers are still running.
            if(queue.length === 0)
            {
                // sleep a bit to give another worker time to add to the queue
                await sleep(100);
                continue;
            }

            // we are gonna start doing work work so track how many workers we have
            numActiveWorkers++;

            try
            {

                // --- ATOMIC TRANSACTION ZONE (No 'await' allowed here!) ---
                let batchTargetUrl = null;
                let matchingBatchRequests = [];

                // check if the next item in the queue (the last item) is a batch request
                // if it is, collect the next up to 50 matching batch requests to process
                // else process the next item
                // Cleanly inspect the end of the queue synchronously
                if(queue.length > 0 && queue.at(-1).hasOwnProperty("batchUrl"))
                {
                    batchTargetUrl = queue.at(-1).batchUrl;

                    const remainingQueueItems = [];
                    for(let i = queue.length - 1; i >= 0; i--)
                    {
                        const item = queue[i];
                        if(item.hasOwnProperty("batchUrl") && item.batchUrl === batchTargetUrl && matchingBatchRequests.length < GOOGLE_API_BATCHING_SIZE)
                        {
                            // Worker takes physical ownership of this item completely right now
                            matchingBatchRequests.push(item);
                        } else
                        {
                            remainingQueueItems.push(item);
                        }
                    }

                    // Instantly update the global queue before yielding control
                    queue.length = 0;
                    for(const item of remainingQueueItems.reverse()) enqueue(item);
                }
                else if(queue.length > 0)
                {
                    // Solo item is pulled out completely synchronously
                    matchingBatchRequests.push(queue.pop());
                }
                // --- END ATOMIC ZONE ---

                // Now it is perfectly safe to await, because the items are completely 
                // wiped out from the global queue. No other worker can see them.
                if(batchTargetUrl)
                {
                    await handleBatchRequest(batchTargetUrl, matchingBatchRequests, workerID);
                }
                else if(matchingBatchRequests.length > 0)
                {
                    await handleSoloRequest(matchingBatchRequests[0], workerID);
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
    async function handleBatchRequest(targetBatchUrl, batchRequests, workerID)
    {
        // make a batch request
        const batchBodyContent = createBatchRequestBody(batchRequests);

        // call it
        await fetchPayload(
            {
                id: "BATCH",
                url: targetBatchUrl,
                contentType: `multipart/mixed; boundary=${GOOGLE_API_BATCHING_BOUNDARY}`,
                body: batchBodyContent
            },
            {
                onSuccess: (responseData, contentType) =>
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
                                processPages(batchRequest, responseJSON, workerID);
                            }
                            else if(RETRYABLE_HTTP_STATUS_CODES.hasOwnProperty(httpResponseCode))
                            {
                                if(batchRequest._retryCount < MAX_RETRY_ATTEMPTS_ITEM)
                                {
                                    batchRequest._retryCount++;
                                    enqueue(batchRequest)
                                }
                                else
                                {
                                    onError({
                                        requestType: "batch",
                                        id: itemId,
                                        message: "retry limit exceeded",
                                        retryCount: batchRequest._retryCount
                                    });
                                }
                            }
                            else
                            {
                                onError({
                                    requestType: "batch",
                                    id: itemId,
                                    message: "not retryable; skipping",
                                    errorCode: httpResponseCode,
                                    errorMessage: httpResponseMessage,
                                    responseJSON: responseJSON
                                });
                            }
                        }
                        else
                        {
                            onError({
                                requestType: "batch",
                                id: itemId,
                                message: "not found in response",
                                request: batchRequest,
                                responseParts: responseParts

                            });
                        }
                    }
                },
                onRetryable: (details) =>
                {
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
                            enqueue(item);
                        }
                        else
                        {
                            onError({
                                requestType: "batch item",
                                id: item.id,
                                message: "retry limit exceeded",
                                retryCount: item._retryCount,
                                retrying: false
                            });
                        }
                    }
                },
                onUnknown: (details) =>
                {
                    onError({
                        ...details,
                        requestType: "batch request",
                        ids: batchRequests.map(i => i.id),
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
                            enqueue(item);
                        }
                        else
                        {
                            onError({
                                requestType: "batch item",
                                id: item.id,
                                message: "retry limit exceeded",
                                retryCount: item._retryCount,
                                retrying: false
                            });
                        }
                    }
                },
                onError: (details) =>
                {
                    onError({
                        ...details,
                        requestType: "batch request",
                        ids: batchRequests.map(i => i.id),
                        retrying: false
                    });
                }
            }
        );

    }

    // Processes a single solo request and handles the response
    async function handleSoloRequest(soloRequest, workerID)
    {
        await fetchPayload(
            soloRequest,
            {
                onSuccess: (responseData, contentType) =>
                {
                    processPages(soloRequest, responseData, workerID);
                },
                onRetryable: (details) =>
                {
                    // retry the request if we can
                    if(soloRequest._retryCount < MAX_RETRY_ATTEMPTS_ITEM)
                    {
                        soloRequest._retryCount++;
                        enqueue(soloRequest);
                    }
                    else
                    {
                        onError({
                            requestType: "solo",
                            id: soloRequest.id,
                            message: "retry limit exceeded",
                            retryCount: soloRequest._retryCount,
                            retrying: false,
                        });
                    }
                },
                onUnknown: (details) =>
                {
                    onError({
                        ...details,
                        requestType: "solo",
                        id: soloRequest.id,
                        retrying: false
                    });
                },
                onError: (details) =>
                {
                    onError({
                        ...details,
                        requestType: "solo",
                        id: soloRequest.id,
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
    function processPages(queueItem, data, workerID)
    {
        // save the page
        queueItem._pages.push(data);
        queueItem._retryCount = 0;

        // if we have another page
        if(data.nextPageToken)
        {
            if(killSwitchTripped) return; // If the kill switch is tripped, do not queue more pages

            onItemNextPage(queueItem.id, queueItem._pages.length, workerID);

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
            enqueue(queueItem);
        }
        else
        {
            // no more pages, so return to user
            onItemDone(queueItem.id, queueItem._pages, workerID);
        }
    }

    // #endregion

    // #region fetch logic

    /**
     * Performs the actual `fetch()` call for one request (solo or batch) and classifies the
     * outcome into exactly one of four handlers:
     *
     *  - `onSuccess`
     *  - `onRetryable` — a retryable HTTP status (429, 500, 502, 503, or 504)
     *  - `onUnknown`   — any any unknown errors
     *  - `onError`     — called for known errors
     */
    async function fetchPayload({id, url, contentType, body}, {onSuccess, onRetryable, onUnknown, onError})
    {
        fetchCount++;

        // Dynamically build Fetch configuration options
        const fetchOptions = {
            method: body !== undefined ? "POST" : "GET",
            headers: {Authorization: `Bearer ${authToken}`},
        };

        // Dynamically apply Content-Type if provided, default to JSON for POSTs if omitted
        if(contentType)
        {
            fetchOptions.headers["Content-Type"] = contentType;
        }
        else if(body !== undefined)
        {
            fetchOptions.headers["Content-Type"] = "application/json";
        }

        // Attach body payload if present (stringifying if it's an object)
        if(body !== undefined)
        {
            fetchOptions.body = typeof body === "object" ? JSON.stringify(body) : body;
        }

        let fetchResponse;

        // fetch in try to catch network errors
        try
        {
            fetchResponse = await fetch(url, fetchOptions);
        }
        catch(error)
        {
            // TO DO: catch transient network errors like a momentary disconnect; BUG-02 (https://gemini.google.com/app/d29994b23668ac56)

            console.debug({
                message: "unknown fetch network error",
                error: error
            })

            return onUnknown({
                id: id,
                url: url,
                message: "unknown fetch network error",
                error: error
            });
        }

        // Even though the request was good, Google/API returned an error code
        if(!fetchResponse.ok)
        {
            // if the request is retryable
            if(RETRYABLE_HTTP_STATUS_CODES.hasOwnProperty(fetchResponse.status))
            {
                // Calculate or apply backoff penalties safely
                handleGlobalCooldownWaitUntilCalculation(fetchResponse.headers.get('Retry-After'));

                // BUG-04 FIX: Evaluate the absolute kill-switch against our distinct failure wave counter
                if(consecutiveFailureCount >= MAX_RETRY_ATTEMPTS_GLOBAL)
                {
                    isDoneProcessingQueue = true;
                    killSwitchTripped = true; // Trip the master circuit breaker

                    return onError({
                        message: `max global retries reached; terminating execution`,
                        retryCount: consecutiveFailureCount
                    });
                }
                else
                {
                    const rateLimitMessage = `global rate limit error; pausing until ${(new Date(globalCooldownWaitUntil)).toLocaleString()} (${Math.max(0, Math.ceil((globalCooldownWaitUntil - Date.now()) / 1000))} seconds) (wave ${consecutiveFailureCount}/${MAX_RETRY_ATTEMPTS_GLOBAL})`;

                    console.debug({
                        message: rateLimitMessage,
                        globalRetryCount: consecutiveFailureCount
                    });

                    return onRetryable({
                        message: rateLimitMessage,
                        globalRetryCount: consecutiveFailureCount
                    });
                }
            }
            else
            {
                console.debug({
                    id: id,
                    url: url,
                    message: "unknown fetch google error",
                    fetchResponse: fetchResponse
                });

                return onUnknown({
                    id: id,
                    url: url,
                    message: "unknown fetch google error",
                    fetchResponse: fetchResponse
                });
            }
        }
        else
        {
            // SUCCESS BRANCH: The request cleared perfectly!
            // Damping down the failure metrics incrementally instead of instantly zeroing them out.
            // This stops a "flapping" API from tricking the system into an infinite loop.
            if(consecutiveFailureCount > 0)
            {
                consecutiveFailureCount--;
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
    function makeUserFunctionSafeToCall(userFunction, functionName, allowAfterKillSwitch = false)
    {
        if(typeof userFunction !== "function")
        {
            throw new TypeError(`The '${functionName}' parameter must be a function.`);
        }

        return (...args) =>
        {
            // no point in calling this function if the kill switch has been tripped because execution has stopped
            if(allowAfterKillSwitch || !killSwitchTripped)
            {
                try
                {
                    userFunction(...args);
                }
                catch(error)
                {
                    console.error(`Error in user-provided function '${functionName}':`, error);
                }
            }
        }
    }

    // Logs a message via `console.debug`, but only when the `debug` option was enabled.
    function debugLog(message)
    {
        if(DEBUG)
        {
            console.debug(message);
        }
    }
    
    // Returns a promise that resolves after a fixed delay.
    function sleep(ms)
    {
        debugLog(`Sleeping for ${ms} milliseconds...`);
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
    
    // Uses exponential backoff to calculate a new global cooldown window after a retryable HTTP response.
    function handleGlobalCooldownWaitUntilCalculation (retryAfterOverride)
    {
        const now = Date.now();

        // ARCH-01 FIX: If we are already in an active cooldown window, ignore subsequent concurrent errors.
        // The first worker to hit the error sets the penalty; the other 3 workers register it but do not stack it.
        if(now < globalCooldownWaitUntil) return;

        // If a significant amount of time has passed since our last failure, reset our backoff memory
        if(now - globalCooldownWaitUntil > 10000)
        {
            consecutiveFailureCount = 0;
        }

        // Increment the true backoff wave counter
        consecutiveFailureCount++;

        if(retryAfterOverride)
        {
            // Respect explicit headers (convert seconds to ms)
            const parsedSeconds = parseInt(retryAfterOverride, 10);
            if(!isNaN(parsedSeconds))
            {
                globalCooldownWaitUntil = now + (parsedSeconds * 1000);
            }
            else
            {
                // Try parsing as HTTP-date format (e.g., "Wed, 21 Oct 2026 07:28:00 GMT")
                const parsedDate = Date.parse(retryAfterOverride);
                if(!isNaN(parsedDate))
                {
                    globalCooldownWaitUntil = parsedDate;
                }
                else
                {
                    // Fall back to exponential calculation if both formats fail
                    const baseDelay = 2000;
                    const exponentialDelay = baseDelay * Math.pow(2, consecutiveFailureCount - 1);
                    const finalDelay = Math.min(exponentialDelay, MAX_RETRY_DELAY * 1000);

                    globalCooldownWaitUntil = now + finalDelay;
                }
            }
        }
        else
        {
            // Calculate smooth exponential growth: 2s, 4s, 8s, 16s... up to your max cap
            const baseDelay = 2000;
            const exponentialDelay = baseDelay * Math.pow(2, consecutiveFailureCount - 1);
            const finalDelay = Math.min(exponentialDelay, MAX_RETRY_DELAY * 1000);

            globalCooldownWaitUntil = now + finalDelay;
        }
    }

    // Pushes an item back onto the shared queue for (re)processing.
    function enqueue(item)
    {
        if(killSwitchTripped)
        {
            return;
        }

        queue.push(item);
    }

    // #endregion
}
