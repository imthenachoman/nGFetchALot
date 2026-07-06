# nGFetchALot <!-- omit from toc -->

A small library for optimized Google API requests, including batch requests, with automatic pagination and exponentially backedoff retries.

## Table of Contents <!-- omit from toc -->

- [What it does](#what-it-does)
- [Quick start](#quick-start)
- [Batching many requests together](#batching-many-requests-together)
- [Cancelling a run](#cancelling-a-run)
- [Queue item](#queue-item)
  - [Solo request](#solo-request)
  - [Batch request](#batch-request)
- [Options](#options)
- [Callbacks](#callbacks)
  - [`onItemDone` argument shapes](#onitemdone-argument-shapes)
  - [`onEvent` argument shapes](#onevent-argument-shapes)

## What it does

You call it with an array of requests and a few callbacks. It handles the rest:

- Runs several requests at once using a small pool of workers (`maxWorkers`)
- Groups requests that share a `batchUrl` into a single Google API batch call instead of one HTTP request per item
- Follows `nextPageToken` automatically until each item's results are fully collected
- Retries failed items on their own using exponential backoff when Google starts rate-limiting
- Can be cancelled mid-run

It's plain JavaScript with no dependencies, built to run client side in a browser — for example, inside the client-side HTML of a Google Apps Script web app.

## Quick start

```js
const folderIds = ['1AbCdEf...', '2GhIjKl...', '3MnOpQr...'];

const job = nGFetchALot({
    authToken: oauthToken,
    queue: folderIds.map(folderId => ({
        id: folderId,
        url: `https://www.googleapis.com/drive/v3/files?q='${folderId}'+in+parents&pageSize=100`
    })),
    onItemDone: ({id, success, pages, message}) => console.log([id, success, pages, message]),
    onQueueDone: () => console.log('queue done'),
    onEvent: ({type, message, details, id}) => console.log([type, message, details, id]),
});

await job.done; // optional — wait for the whole queue to finish
```

The three folders are listed concurrently. Any folder with more than 100 files automatically gets its next page fetched too — `pages` in `onItemDone` already has everything merged together, no pagination code needed on your end.

## Batching many requests together

If you have a lot of small lookups against the same API, group them with `batchUrl` so they go out as one HTTP call instead of many:

```js
const fileIds = ['1aBcD...', '2eFgH...', '3iJkL...']; // could be hundreds

const job = nGFetchALot({
    authToken: oauthToken,
    queue: fileIds.map(fileId => ({
        id: fileId,
        batchUrl: 'https://www.googleapis.com/batch/drive/v3',
        apiPath: `/drive/v3/files/${fileId}?fields=id,name,modifiedTime`
    })),
    onItemDone: ({id, success, pages, message}) => console.log([id, success, pages, message]),
    onQueueDone: () => console.log('queue done'),
    onEvent: ({type, message, details, id}) => console.log([type, message, details, id]),
});
```

Items that share the same `batchUrl` are grouped into batches of up to `batchSize` (default 50). Three hundred file lookups becomes six HTTP requests instead of three hundred.

## Cancelling a run

`nGFetchALot` hands back a controller synchronously — you don't have to await anything to get hold of it, so you can wire up a cancel button the moment a job starts:

```js
let job;

startButton.onclick = () => {
    job = nGFetchALot({...});
};

cancelButton.onclick = () => {
    job.stop(); // workers finish their current request, then stop picking up new ones
};

// job.done resolves once every worker has actually exited
```

## Queue item

Every item in `queue` is either a solo request or a batch request.

### Solo request

```js
// Solo — fetched on its own
{
    id: 'unique-id',
    url: 'https://...',
    body: optionalBody,
    contentType: optionalContentType
},
{
    id: 'unique-id',
    url: 'https://...',
}
```

### Batch request

```js
// Batch — grouped with other items that share the same batchUrl
{
    id: 'unique-id',
    batchUrl: 'https://www.googleapis.com/batch/...',
    apiPath: '/drive/v3/files/abc',
    body: optionalBody
    contentType: optionalContentType
},
{
    id: 'unique-id',
    batchUrl: 'https://www.googleapis.com/batch/...',
    apiPath: '/drive/v3/files/abc',
},
{
    id: 'unique-id',
    batchUrl: 'https://www.googleapis.com/batch/...',
    apiPath: '/drive/v3/files/abc',
    body: optionalBody
}

```

`body` can be a string or a plain object (objects are JSON-stringified for you). If `body` is present, the request is sent as `POST`; otherwise it's a `GET`.

## Options

| Option           | Default    | What it does                                                                    |
| ---------------- | ---------- | ------------------------------------------------------------------------------- |
| `authToken`      | *required* | OAuth2 bearer token sent with every request                                     |
| `queue`          | *required* | The [request(s)](#queue-item) to process                                      |
| `maxWorkers`     | `4`        | How many requests can be in flight at once                                      |
| `maxItemRetry`   | `4`        | How many times a single failing item is retried before it's given up on         |
| `maxGlobalRetry` | `4`        | How many rate-limit "waves" the whole pool tolerates before stopping entirely   |
| `maxRetryDelay`  | `30`       | Max seconds for exponential backoff delay when no `Retry-After` header is given |
| `batchSize`      | `50`       | Max items grouped into one batch call                                           |

## Callbacks

| Callback                                    | Fires when                                  | Arguments                             |
| ------------------------------------------- | ------------------------------------------- | ------------------------------------- |
| [`onItemDone`](#onitemdone-argument-shapes) | An item has been fully processed, or failed | `({id, success, pages, message})`     |
| `onQueueDone`                               | The whole queue finished                    | `()`                                  |
| [`onEvent`](#onevent-argument-shapes)       | An event occured                            | `({type, message, details, id})` |


### `onItemDone` argument shapes

```
{
    id: "...",
    success: `true`,
    pages: `[{...}, {...}, ...]`
}
```

```
{
    id: "...",
    success: `false`,
    message: `batch item retry limit exceeded; skipping`
}
```

```
{
    id: "...",
    success: `false`,
    message: `batch item failure; not retryable; skipping`
}
```

```
{
    id: "...",
    success: `false`,
    message: `batch item not found in response; skipping`
}
```

```
{
    id: "...",
    success: `false`,
    message: `batch request failure; skipping`
}
```

```
{
    id: "...",
    success: `false`,
    message: `solo item retry limit exceeded; skipping`
}
```

```
{
    id: "...",
    success: `false`,
    message: `solo request failure; skipping`
}
```

### `onEvent` argument shapes

```
{
    type: "info",
    message: "waiting for other workers to finish; sleeping for XX milliseconds...",
}
```

```
{
    type: "info",
    message: "cooling down before retrying; sleeping for XX milliseconds...",
}
```

```
{
    type: "info",
    message: "getting next page",
    details: {
        id: "...",
        pagesSoFar: #
    },
    id: "..."
}
```

```
{
    type: "warning",
    message: "batch item request error; requeuing request",
    details: {
        kind: "batch item",
        httpResponseCode: httpResponseCode,
        httpResponseMessage: httpResponseMessage,
        responseJSON: responseJSON
    },
    id: "..."
}
```

```
{
    type: "warning",
    message: "batch request error: unknown fetch error; requeuing requests; pausing until [date/time] (XX seconds) (attempt #/#)",
    details: {
        kind: "network",
        message: "...",
        errorName: "..."
    },
    id: ["...", "...", ...] // array of the IDs of all the requests in this batch
}
```

```
{
    type: "warning",
    message: "batch request error: google failure with retrable code; requeuing requests; pausing until [date/time] (XX seconds) (attempt #/#)",
    details: {
        kind: "http",
        message: `HTTP [status] ([text]),
        httpResponseCode: "...",
        httpResponseMessage: "...",
        retryAfter: "..."
    },
    id: ["...", "...", ...] // array of the IDs of all the requests in this batch
}
```

```
{
    type: "warning",
    message: "solo request error: unknown fetch error; requeuing request; pausing until [date/time] (XX seconds) (attempt #/#)",
    details: {
        kind: "network",
        message: "...",
        errorName: "..."
    },
    id: "..."
}
```

```
{
    type: "warning",
    message: "solo request error: google failure with retrable code; requeuing request; pausing until [date/time] (XX seconds) (attempt #/#)",
    details: {
        kind: "http",
        message: `HTTP [status] ([text]),
        httpResponseCode: "...",
        httpResponseMessage: "...",
        retryAfter: "..."
    },
    id: "..."
}
```

```
{
    type: "error",
    message: "batch item request error; not retryable; skipping",
    details: {
        kind: "batch item",
        httpResponseCode: httpResponseCode,
        httpResponseMessage: httpResponseMessage,
        responseJSON: responseJSON
    },
    id: "..."
}
```

```
{
    type: "error",
    message: "batch item not found in response; skipping",
    details: {
        responseData: responseData,
        boundary: boundary,
        responseParts: responseParts
    },
    id: "..."
}
```

```
{
    type: "error",
    message: "batch request failure: unknown google error; skipping requests",
    details: {
        kind: "http",
        message: `HTTP [status] ([text]),
        httpResponseCode: "...",
        httpResponseMessage: "...",
        retryAfter: "..."
    },
    id: ["...", "...", ...] // array of the IDs of all the requests in this batch
}
```

```
{
    type: "error",
    message: "solo request failure: unknown google error; skipping request",
    details: {
        kind: "http",
        message: `HTTP [status] ([text]),
        httpResponseCode: "...",
        httpResponseMessage: "...",
        retryAfter: "..."
    },
    id: "..."
}
```