/**
 * nGFetchALot
 * 
 * A small library for optimized Google API requests, including batch requests,
 * with automatic pagination and exponentially backedoff retries.
 * 
 * author: imthenachoman@gmail.com
 * github: https://github.com/imthenachoman/nGFetchALot/
 */

function nGFetchALot({
    authToken,
    queue: userQueue = [],
    onItemDone: userOnItemDone = ({id, success, pages, message}) => console.log([id, success, pages, message]),
    onQueueDone: userOnQueueDone = () => console.log('queue done'),
    onEvent: userOnEvent = ({type, message, details, id}) => console.log([type, message, details, id]),
    maxWorkers: MAX_CONCURRENT_WORKERS = 4,
    maxGlobalRetry: MAX_RETRY_ATTEMPTS_GLOBAL = 4,
    maxItemRetry: MAX_RETRY_ATTEMPTS_ITEM = 4,
    maxRetryDelay: MAX_RETRY_DELAY = 30,
    batchSize: GOOGLE_API_BATCHING_SIZE = 50,
})
{
    // #region make user provided functions safe to call

    const onItemDone = makeUserFunctionSafeToCall(userOnItemDone, "onItemDone");
    const onQueueDone = makeUserFunctionSafeToCall(userOnQueueDone, "onQueueDone");
    const onEvent = makeUserFunctionSafeToCall(userOnEvent, "onEvent");

    // #endregion

    // #region global variables

    // settings/constants
    const GOOGLE_API_BATCHING_BOUNDARY = "batch_nGFetchALot_request_boundary";
    const RETRYABLE_HTTP_STATUS_CODES = {429: true, 500: true, 502: true, 503: true, 504: true};
    const TEXT_ENCODER = new TextEncoder();

    let isDoneProcessingQueue = false;            // if we need to stop all workers, for whatever reason
    let numActiveWorkers = 0;                     // track how many concurrent workers we have

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

    // make a copy of the queue
    // store the queue in reverse order from the input
    // this way we can pop from the end to get the next item in the queue
    // and we can push to the end for retries so they get queued up faster (on the next pop)
    const queue = userQueue
        .map((queueItem, index) =>
        {
            // we need to make sure each item has a url or batchUrl
            let urlToValidate = queueItem.url || queueItem.batchUrl;

            if(!queueItem.id) throw new TypeError(`Missing id in queue item ${index}: ${JSON.stringify(queueItem)}`);
            if(!urlToValidate) throw new TypeError(`Missing url/batchUrl in queue item ${index}: ${JSON.stringify(queueItem)}`);

            if(queueItem.batchUrl)
            {
                if(!queueItem.apiPath) throw new TypeError(`Missing apiPath in batch queue item ${index}: ${JSON.stringify(queueItem)}`);
                // for batch urls, we want to validate the full path
                else urlToValidate = queueItem.batchUrl + queueItem.apiPath;
            }

            try
            {
                new URL(urlToValidate);
            }
            catch
            {
                throw new TypeError(`Invalid URL in queue item ${index}: ${urlToValidate}`);
            }

            return {
                ...queueItem,
                _pages: [],
                _retryCount: 0
            }
        })
        .reverse();

    // do the main magic
    const done = (async () =>
    {
        // wrap each worker so that no matter *why* it throws, every other
        // worker is told to stop immediately, before we even finish awaiting
        async function runWorker(workerID)
        {
            try
            {
                await worker(workerID);
            }
            catch(error)
            {
                // flip this synchronously, the instant we know about the failure,
                // so siblings exit on their next loop-top check rather than
                // running until Promise.allSettled happens to notice
                isDoneProcessingQueue = true;
                throw error;
            }
        }

        const results = await Promise.allSettled(
            Array.from(
                {
                    length: MAX_CONCURRENT_WORKERS
                },
                (_, index) => runWorker(`worker${index + 1}`)
            )
        );

        const rejected = results.filter(r => r.status === "rejected");

        if(rejected.length > 0)
        {
            killSwitchTripped = true;
            isDoneProcessingQueue = true;

            // surface the first failure to the caller; log any others so they
            // aren't silently lost (multiple rejections should be rare given
            // the cooldown dedup in handleRetryLogic, but not impossible)
            for(let i = 1; i < rejected.length; i++)
            {
                console.error("additional worker failure (not surfaced via done):", rejected[i].reason);
            }

            throw rejected[0].reason;
        }

        onQueueDone();
    })();

    // return a controller the user can use to stop the queue processing
    return {
        stop: () =>
        {
            if(killSwitchTripped) return false; // already tripped, no need to trip again

            isDoneProcessingQueue = true;
            killSwitchTripped = true;

            return true; // successfully tripped the kill switch
        },
        done: done
    };

    // #endregion

    // #region worker and processing stuff

    // Each worker runs in a loop until the queue is empty and all workers are done, or until the kill switch is tripped
    async function worker(workerID)
    {
        while(!isDoneProcessingQueue)
        {
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
                await sleep(100, 'waiting for other workers to finish');
                continue;
            }

            // calculate how much time is left to wait for the cool down
            const baseWaitTime = globalCooldownWaitUntil - Date.now();
            // if we still have time left
            if(baseWaitTime > 0)
            {
                // add a worker specific jitter so all the workers don't wake at the same time
                await sleep(baseWaitTime + (Math.random() * 1000), 'cooling down before retrying');
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
                if(queue.length > 0 && queue.at(-1).hasOwnProperty("batchUrl"))
                {
                    batchTargetUrl = queue.at(-1).batchUrl;

                    // get the next up to 50 matching requests
                    for(let i = queue.length - 1; i >= 0 && matchingBatchRequests.length < GOOGLE_API_BATCHING_SIZE; i--)
                    {
                        if(queue[i].batchUrl === batchTargetUrl)
                        {
                            matchingBatchRequests.push(queue[i]);
                            queue[i] = null; // mark for removal, no shift
                        }
                    }

                    // single O(n) compaction pass instead of up to 50 O(n) shifts
                    let writeIndex = 0;
                    for(let i = 0; i < queue.length; i++)
                    {
                        if(queue[i] !== null) queue[writeIndex++] = queue[i];
                    }
                    queue.length = writeIndex;
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
                    await handleBatchRequest(batchTargetUrl, matchingBatchRequests);
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
    async function handleBatchRequest(targetBatchUrl, batchRequests)
    {
        // make a batch request
        const batchBodyContent = createBatchRequestBody(batchRequests);
        const ret = await fetchPayload({
            id: "BATCH",
            url: targetBatchUrl,
            contentType: `multipart/mixed; boundary=${GOOGLE_API_BATCHING_BOUNDARY}`,
            body: batchBodyContent
        });

        if(ret.status == "success")
        {
            const {responseData, reponseContentType} = ret;

            // the batch request was a success
            // extract and handle each batch request item
            const boundaryMatch = reponseContentType.match(/boundary=([^;]+)/i);
            const boundary = boundaryMatch ? boundaryMatch[1].trim().replace(/['"]/g, "") : null;

            const responseParts = parseBatchRequestResponse(responseData, boundary);

            // match parsed json respones against the original batch requests
            for(let i = batchRequests.length - 1; i >= 0; i--)
            {
                const queueItem = batchRequests[i];
                const itemId = queueItem.id;

                const responseMatch = responseParts[itemId];

                if(responseMatch)
                {
                    const {httpResponseCode, httpResponseMessage, retryAfter, responseJSON} = responseMatch;

                    // if the individual response was good, we can process it
                    if(httpResponseCode == 200)
                    {
                        processPages(queueItem, responseJSON);
                    }
                    else if(RETRYABLE_HTTP_STATUS_CODES.hasOwnProperty(httpResponseCode))
                    {
                        onEvent({
                            type: "warning",
                            message: `batch item request error; requeuing request`,
                            details: {
                                httpResponseCode: httpResponseCode,
                                httpResponseMessage: httpResponseMessage,
                                responseJSON: responseJSON
                            },
                            id: queueItem.id
                        });

                        // only push if we haven't hit retry limit
                        if(queueItem._retryCount < MAX_RETRY_ATTEMPTS_ITEM)
                        {
                            queueItem._retryCount++;
                            queue.push(queueItem);
                        }
                        else
                        {
                            onItemDone({
                                id: queueItem.id,
                                success: false,
                                message: "batch item retry limit exceeded; skipping"
                            });
                        }
                    }
                    else // failure
                    {
                        onEvent({
                            type: "error",
                            message: `batch item request error; not retryable; skipping`,
                            details: {
                                httpResponseCode: httpResponseCode,
                                httpResponseMessage: httpResponseMessage,
                                responseJSON: responseJSON
                            },
                            id: queueItem.id
                        });

                        onItemDone({
                            id: queueItem.id,
                            success: false,
                            message: "batch item failure; not retryable; skipping",
                        });
                    }
                }
                else
                {
                    onEvent({
                        type: "error",
                        message: "batch item not found in response; skipping",
                        details: {
                            responseData: responseData,
                            boundary: boundary,
                            responseParts: responseParts
                        },
                        id: queueItem.id
                    });

                    onItemDone({
                        id: queueItem.id,
                        success: false,
                        message: "batch item not found in response; skipping"
                    });
                }
            }
        }
        else if(ret.status == "retry")
        {
            onEvent({
                type: "warning",
                message: `batch request error: ${ret.message}; requeuing requests; pausing until ${(new Date(ret.waitUntil)).toLocaleString()} (${Math.max(0, Math.ceil((ret.waitUntil - Date.now()) / 1000))} seconds) (attempt ${ret.failureCount}/${MAX_RETRY_ATTEMPTS_GLOBAL})`,
                details: ret.details,
                id: batchRequests.map(queueItem => queueItem.id)
            });

            // we need to reque the batch requests that we can
            // do in reverse order to preserve priority
            for(let i = batchRequests.length - 1; i >= 0; i--)
            {
                const queueItem = batchRequests[i];

                // only push if we haven't hit retry limit
                if(queueItem._retryCount < MAX_RETRY_ATTEMPTS_ITEM)
                {
                    queueItem._retryCount++;
                    queue.push(queueItem);
                }
                else
                {
                    onItemDone({
                        id: queueItem.id,
                        success: false,
                        message: "batch item retry limit exceeded; skipping",
                    });
                }
            }
        }
        else if(ret.status == "failure")
        {
            onEvent({
                type: "error",
                message: `batch request failure: ${ret.message}; skipping requests`,
                details: ret.details,
                id: batchRequests.map(queueItem => queueItem.id)
            });

            for(const queueItem of batchRequests)
            {
                onItemDone({
                    id: queueItem.id,
                    success: false,
                    message: `batch request failure; skipping`,
                });
            }
        }

    }

    // Processes a single solo request and handles the response
    async function handleSoloRequest(queueItem)
    {
        const ret = await fetchPayload(queueItem);

        if(ret.status == "success")
        {
            processPages(queueItem, ret.responseData)
        }
        else if(ret.status == "retry")
        {
            onEvent({
                type: "warning",
                message: `solo request error: ${ret.message}; requeuing request; pausing until ${(new Date(ret.waitUntil)).toLocaleString()} (${Math.max(0, Math.ceil((ret.waitUntil - Date.now()) / 1000))} seconds) (attempt ${ret.failureCount}/${MAX_RETRY_ATTEMPTS_GLOBAL})`,
                details: ret.details,
                id: queueItem.id
            });

            // only push if we haven't hit retry limit
            if(queueItem._retryCount < MAX_RETRY_ATTEMPTS_ITEM)
            {
                queueItem._retryCount++;
                queue.push(queueItem);
            }
            else
            {
                onItemDone({
                    id: queueItem.id,
                    success: false,
                    message: "solo item retry limit exceeded; skipping",
                });
            }
        }
        else if(ret.status == "failure")
        {
            onEvent({
                type: "error",
                message: `solo request failure: ${ret.message}; skipping request`,
                details: ret.details,
                id: queueItem.id
            });

            onItemDone({
                id: queueItem.id,
                success: false,
                message: `solo request failure; skipping`,
            });
        }
    }

    // Builds the raw multipart/mixed request body for a Google API batch call
    function createBatchRequestBody(batchRequests)
    {
        let batchBodyContent = "";

        batchBodyContent += batchRequests.map(queueItem =>
        {
            // create the batch request for this request
            const hasBody = queueItem.body !== undefined;
            const subMethod = hasBody ? 'POST' : 'GET'; // if the original request has a body, we need to change the batch request to post and 
            const subBody = hasBody ? (typeof queueItem.body === 'object' ? JSON.stringify(queueItem.body) : queueItem.body) : '';

            let partLines = [
                `--${GOOGLE_API_BATCHING_BOUNDARY}`,
                `Content-Type: application/http`,
                `Content-ID: <item-${queueItem.id}>`,
                ``,                                               // Blank line separating multipart headers from the inner HTTP request
                `${subMethod} ${queueItem.apiPath}`    // Inner HTTP Request Line
            ];

            // 2. Inner HTTP Headers (Must follow the Request Line directly)
            if(hasBody)
            {
                const byteLength = TEXT_ENCODER.encode(subBody).length;

                partLines.push(`Content-Type: ${queueItem.contentType || 'application/json'}`);
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
    function processPages(queueItem, responseJSON)
    {
        // save the page
        queueItem._pages.push(responseJSON);
        queueItem._retryCount = 0;

        // if we have another page
        if(responseJSON.nextPageToken)
        {
            if(killSwitchTripped) return; // If the kill switch is tripped, do not queue more pages

            onEvent({
                type: "info",
                message: "getting next page",
                details: {
                    id: queueItem.id,
                    pagesSoFar: queueItem._pages.length
                },
                id: queueItem.id
            });

            // update the url with the next page token
            if(queueItem.url)
            {
                queueItem.url = setOrAppendURLQueryParameter(queueItem.url, 'pageToken', responseJSON.nextPageToken);
            }
            else if(queueItem.apiPath)
            {
                queueItem.apiPath = setOrAppendURLQueryParameter(queueItem.apiPath, 'pageToken', responseJSON.nextPageToken);
            }

            // requeue it
            queue.push(queueItem);
        }
        else
        {
            // no more pages, so return to user
            onItemDone({
                id: queueItem.id,
                success: true,
                pages: queueItem._pages
            });
        }
    }

    // #endregion

    // #region fetch logic

    // Performs the actual `fetch()` call for one request (solo or batch)
    async function fetchPayload({id, url, contentType, body})
    {
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
            // TO DO: catch transient network errors like a momentary disconnect
            // for now, just retry everything

            console.error(`unknown fetch error for ${url}; retrying:`, error, {id, url, contentType, body});
            return {
                message: "unknown fetch error",
                details: {
                    kind: "network",
                    message: error?.message || String(error),
                    errorName: error?.name || null
                },
                ...handleRetryLogic()
            };
        }

        // Even though the request was good, Google/API returned an error code
        if(!fetchResponse.ok)
        {
            // if the request is retryable
            if(RETRYABLE_HTTP_STATUS_CODES.hasOwnProperty(fetchResponse.status))
            {
                console.error(`google failure with retrable code ${fetchResponse.status}`, fetchResponse, {id, url, contentType, body});
                return {
                    message: "google failure with retrable code",
                    details: {
                        kind: "http",
                        message: `HTTP ${response.status}${response.statusText ? ` (${response.statusText})` : ""}`,
                        httpResponseCode: response.status,
                        httpResponseMessage: response.statusText || null,
                        retryAfter: fetchResponse.headers.get('Retry-After')
                    },
                    ...handleRetryLogic(fetchResponse.headers.get('Retry-After'))
                };
            }
            else
            {
                // if it's not retryable, 
                console.error('unknown google error', fetchResponse, {id, url, contentType, body});
                return {
                    status: "failure",
                    message: "unknown google error",
                    details: {
                        kind: "http",
                        message: `HTTP ${response.status}${response.statusText ? ` (${response.statusText})` : ""}`,
                        httpResponseCode: response.status,
                        httpResponseMessage: response.statusText || null,
                        retryAfter: fetchResponse.headers.get('Retry-After')
                    }
                }
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
                // return the text/body response
                return {
                    status: "success",
                    responseData: await fetchResponse.text(),
                    reponseContentType: reponseContentType
                };
            }
            else
            {
                // return the json response
                return {
                    status: "success",
                    responseData: await fetchResponse.json(),
                    reponseContentType: reponseContentType
                };
            }
        }
    }

    // Uses exponential backoff to calculate a new global cooldown window after a retryable HTTP response.
    function handleRetryLogic(retryAfterOverride)
    {
        const now = Date.now();

        // ARCH-01 FIX: If we are already in an active cooldown window, ignore subsequent concurrent errors.
        // The first worker to hit the error sets the penalty; the other 3 workers register it but do not stack it.
        if(now < globalCooldownWaitUntil) return {
            status: "retry",
            waitUntil: globalCooldownWaitUntil,
            failureCount: consecutiveFailureCount,
        };

        // If a significant amount of time has passed since our last failure, reset our backoff memory
        if(now - globalCooldownWaitUntil > 10000)
        {
            consecutiveFailureCount = 0;
        }

        // Increment the true backoff wave counter
        consecutiveFailureCount++;

        // if we've hit the max, we need to kill everything
        if(consecutiveFailureCount >= MAX_RETRY_ATTEMPTS_GLOBAL)
        {
            isDoneProcessingQueue = true;
            killSwitchTripped = true; // Trip the master circuit breaker

            throw new Error("max global retries reached; terminating execution");
        }
        else
        {
            // we can retry
            // calculate retry time
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

            return {
                status: "retry",
                waitUntil: globalCooldownWaitUntil,
                failureCount: consecutiveFailureCount,
            };
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

    // Returns a promise that resolves after a fixed delay.
    function sleep(ms, reason)
    {
        onEvent({
            type: "info",
            message: `${reason}; sleeping for ${ms} milliseconds...`
        });
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

    // #endregion
}