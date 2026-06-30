# nGFetchALot

A small library for fetching lots of Google API requests at once — concurrently, in batches, with automatic pagination and retries — without writing that plumbing yourself.

## What it does

You give it a list of requests and a few callbacks. It handles the rest:

- Runs several requests at once using a small pool of workers (`maxWorkers`)
- Groups requests that share a `batchUrl` into a single Google API batch call instead of one HTTP request per item
- Follows `nextPageToken` automatically until each item's results are fully collected
- Retries failed items on their own, and separately backs off the *entire* pool when Google starts rate-limiting you
- Can be cancelled mid-run

It's plain JavaScript with no dependencies, built to run in a browser — for example, inside the client-side HTML of a Google Apps Script web app.

## Quick start

```js
const folderIds = ['1AbCdEf...', '2GhIjKl...', '3MnOpQr...'];

const job = nGFetchALot({
    authToken: oauthToken,
    queue: folderIds.map(folderId => ({
        id: folderId,
        url: `https://www.googleapis.com/drive/v3/files?q='${folderId}'+in+parents&pageSize=100`
    })),
    onItemNextPage: (id, numPages) => console.log(`${id}: fetching page ${numPages + 1}...`),
    onItemDone: (id, pages) => {
        const allFiles = pages.flatMap(page => page.files);
        console.log(`${id}: found ${allFiles.length} files across ${pages.length} page(s)`);
    },
    onError: (data) => console.error('Failed:', data),
    onQueueDone: (fetchCount) => console.log(`Done — ${fetchCount} requests sent.`)
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
    onItemDone: (id, pages) => console.log(id, pages[0].name),
    onError: (data) => console.error(data)
});
```

Items that share the same `batchUrl` are grouped into batches of up to `batchSize` (default 50). Three hundred file lookups becomes six HTTP requests instead of three hundred.

## Cancelling a run

`nGFetchALot` hands back a controller synchronously — you don't have to await anything to get hold of it, so you can wire up a cancel button the moment a job starts:

```js
let job;

startButton.onclick = () => {
    job = nGFetchALot({
        authToken: oauthToken,
        queue: buildLargeQueue(),
        onItemDone: () => updateProgressBar(),
        onQueueDone: () => showMessage('Finished!'),
        onError: (data) => console.error(data)
    });
};

cancelButton.onclick = () => {
    job.stop(); // workers finish their current request, then stop picking up new ones
};

// job.done resolves once every worker has actually exited
```

## Request shapes

Every item in `queue` is either a solo request or a batch request:

```js
// Solo — fetched on its own
{
    id: 'unique-id',
    url: 'https://...',
    body: optionalBody,
    contentType: optionalType
}

// Batch — grouped with other items that share the same batchUrl
{
    id: 'unique-id',
    batchUrl: 'https://www.googleapis.com/batch/...',
    apiPath: '/drive/v3/files/abc',
    body: optionalBody
}
```

`body` can be a string or a plain object (objects are JSON-stringified for you). If `body` is present, the request is sent as `POST`; otherwise it's a `GET`.

## Options

| Option | Default | What it does |
|---|---|---|
| `authToken` | *required* | OAuth2 bearer token sent with every request |
| `queue` | `[]` | The requests to process |
| `maxWorkers` | `4` | How many requests can be in flight at once |
| `maxItemRetry` | `4` | How many times a single failing item is retried before it's given up on |
| `maxGlobalRetry` | `4` | How many rate-limit "waves" the whole pool tolerates before stopping entirely |
| `maxRetryDelay` | `30` | Cap, in seconds, on backoff delay when no `Retry-After` header is given |
| `batchSize` | `50` | Max items grouped into one batch call |
| `batchBoundary` | `batch_nGFetchALot_request_boundary` | MIME boundary used in batch request bodies — rarely needs changing |
| `debug` | `false` | Logs detailed step-by-step worker activity to the console |

## Callbacks

| Callback | Fires when | Arguments |
|---|---|---|
| `onItemDone` | An item's last page has arrived | `(id, pages, workerID)` |
| `onItemNextPage` | Another page is on the way for an item | `(id, numPages, workerID)` |
| `onQueueDone` | The whole queue finished normally | `(fetchCount)` |
| `onError` | An item was given up on, or the run stopped early | `(data)` — shape varies by failure |

## How retries work

Two layers, working independently. If one item keeps failing, it's retried up to `maxItemRetry` times and then reported through `onError` — nothing else is affected. If Google starts rate-limiting you, every worker pauses together for a backoff period that grows with each wave of failures; if that keeps happening past `maxGlobalRetry` waves, the whole run stops, `onError` fires to explain why, and `onQueueDone` is skipped.

Status codes `429`, `500`, `502`, `503`, and `504` are treated as retryable. A `409` conflict is not retried automatically, since resending the exact same request rarely resolves a real conflict.
