# Soulseek.jvm — Public API Reference

This document describes the complete public API of Soulseek.jvm, a Java library
for the Soulseek peer-to-peer network. It covers every exported type, every
method, every event, and every extension point. It is written for two readers:
a developer building an application (a UI, a daemon, a bot) on the library, and
an agent that needs an exact, complete description of the surface.

The document moves from common to niche. Read the quickstart to download a file
in five minutes. Read the concepts section to understand the design once and
predict the rest. Read the facet reference for every method. Read the SPI and
recipe sections when you need to replace a default.

Everything in this document is the current public surface. Public entry points
and shared control contracts stay in `dev.slsk`; requests and snapshots are
grouped by the capability that owns them.

Blocking methods respond to thread interruption and offer a `Duration` deadline
overload instead (see [3.2](#32-blocking-calls-virtual-threads-interruption)).

---

## Table of contents

1. [The library at a glance](#1-the-library-at-a-glance)
2. [Quickstart: download a file](#2-quickstart-download-a-file)
3. [Core concepts](#3-core-concepts)
4. [Building a client: `SoulseekBuilder`](#4-building-a-client-soulseekbuilder)
5. [Facet reference](#5-facet-reference)
   - [5.1 `connection()`](#51-connection)
   - [5.2 `search()`](#52-search)
   - [5.3 `downloads()`](#53-downloads)
   - [5.4 `uploads()`](#54-uploads)
   - [5.5 `users()`](#55-users)
   - [5.6 `rooms()` and `privateRooms()`](#56-rooms-and-privaterooms)
   - [5.7 `chat()`](#57-chat)
   - [5.8 `shares()`](#58-shares)
   - [5.9 `me()`](#59-me)
   - [5.10 `diagnostics()`](#510-diagnostics)
6. [The SPI: what you implement](#6-the-spi-what-you-implement)
7. [Utility types](#7-utility-types)
8. [Exceptions](#8-exceptions)
9. [Building a UI on this API](#9-building-a-ui-on-this-api)
10. [Recipes](#10-recipes)
11. [Appendix: type index](#11-appendix-type-index)

---

## 1. The library at a glance

| Fact | Value |
|---|---|
| Maven coordinates | `dev.slsk:slsk-jvm:2.1.0` |
| Distribution | Local Maven repository only. Run `mvn install` in the library checkout. Not published to Maven Central. |
| Java version | 25 or later |
| JPMS module | `dev.slsk.soulseek` |
| Runtime dependencies | None |
| License | GPL-3.0-only, based on Soulseek.NET 10.0.2 |
| Root package | `dev.slsk` |
| Threading model | Blocking calls, designed for virtual threads |
| Root type | [`dev.slsk.Soulseek`](../src/main/java/dev/slsk/Soulseek.java) |

The module exports these architectural packages:

| Package | Rule |
|---|---|
| [`dev.slsk`](../src/main/java/dev/slsk/package-info.java) | What you call. The root type, ten facets, subscriptions, and attachments. |
| [`dev.slsk.connection`](../src/main/java/dev/slsk/connection/package-info.java) | Server addresses, metadata, and connection state. |
| [`dev.slsk.diagnostics`](../src/main/java/dev/slsk/diagnostics/package-info.java) | Diagnostic levels, metrics, and distributed-mesh snapshots. |
| [`dev.slsk.download`](../src/main/java/dev/slsk/download/package-info.java) | Download requests, policies, and snapshots. |
| [`dev.slsk.events`](../src/main/java/dev/slsk/events/package-info.java) | What you receive. One sealed event hierarchy per facet. |
| [`dev.slsk.exceptions`](../src/main/java/dev/slsk/exceptions/package-info.java) | What can go wrong. An unchecked exception hierarchy. |
| [`dev.slsk.room`](../src/main/java/dev/slsk/room/package-info.java) | Room directory, membership, users, and tickers. |
| [`dev.slsk.search`](../src/main/java/dev/slsk/search/package-info.java) | Search queries, filters, results, files, and lifecycle snapshots. |
| [`dev.slsk.share`](../src/main/java/dev/slsk/share/package-info.java) | Browse responses, shared directories, indexes, and remote paths. |
| [`dev.slsk.spi`](../src/main/java/dev/slsk/spi/package-info.java) | What you implement. Extension points with working defaults. |
| [`dev.slsk.transfer`](../src/main/java/dev/slsk/transfer/package-info.java) | Transfer ids, state, outcomes, progress, priorities, and retry values. |
| [`dev.slsk.upload`](../src/main/java/dev/slsk/upload/package-info.java) | Upload snapshots. |
| [`dev.slsk.user`](../src/main/java/dev/slsk/user/package-info.java) | User identity, presence, profiles, statistics, watches, and browse values. |

Everything else lives under `dev.slsk.internal` and is not exported. No
internal type is reachable from an exported signature. A test enforces this.

In a modular application, declare:

```java
requires dev.slsk.soulseek;
```

---

## 2. Quickstart: download a file

This program connects, searches the network, picks a source, downloads one
file, and prints the outcome.

```java
import java.nio.file.Path;
import java.util.Comparator;
import dev.slsk.Soulseek;
import dev.slsk.download.Download;
import dev.slsk.download.DownloadRequest;
import dev.slsk.search.SearchFile;
import dev.slsk.search.SearchQuery;
import dev.slsk.search.SearchResponse;
import dev.slsk.search.SearchResult;
import dev.slsk.transfer.TransferId;
import dev.slsk.transfer.TransferOutcome;
import dev.slsk.transfer.TransferState;

public class Quickstart {
    public static void main(String[] args) throws Exception {
        try (Soulseek slsk = Soulseek.builder()
                .credentials("your-username", "your-password")
                .applicationMinorVersion(1234) // your build's own number, must be > 100
                .build()) {

            // 1. Connect to the public server and log in. Blocks until online.
            slsk.connection().connect();

            // 2. Search. Blocks until the search completes (about 15 seconds,
            //    or sooner when responses stop arriving).
            SearchResult result = slsk.search()
                    .run(SearchQuery.of("some artist some album"));

            if (result.isEmpty()) {
                System.out.println("Nobody answered.");
                return;
            }

            // 3. Pick a source: a peer with a free slot, fastest first.
            SearchResponse source = result.responses().stream()
                    .filter(SearchResponse::hasFreeSlot)
                    .max(Comparator.comparingLong(SearchResponse::uploadSpeed))
                    .orElse(result.responses().getFirst());
            SearchFile file = source.files().getFirst();

            // 4. Enqueue the download. Returns immediately with an id.
            TransferId id = slsk.downloads().enqueue(
                    DownloadRequest.of(source.user(), file, Path.of("downloads", file.name())));

            // 5. Wait for it to finish, then read the outcome.
            Download done = slsk.downloads().await(id);
            if (done.state() instanceof TransferState.Finished(TransferOutcome outcome)) {
                System.out.println(switch (outcome) {
                    case TransferOutcome.Succeeded s -> "Done: " + s.bytes() + " bytes";
                    case TransferOutcome.Rejected r -> "Refused: " + r.rawMessage();
                    case TransferOutcome.Failed f -> "Failed: " + f.cause();
                    case TransferOutcome.Cancelled c -> "Cancelled";
                });
            }
        }
    }
}
```

Points that matter beyond this example:

- **`build()` does not connect.** `connection().connect(...)` does.
- **`applicationMinorVersion` is required.** The server uses it to tell client
  builds apart. Pick your own number greater than 100 and keep it stable.
- **Credentials are required.** There is no anonymous access. Any
  username/password pair registers the account on first login.
- **The client is process-lifetime.** Build one `Soulseek`, keep it, and close
  it on shutdown. Reconnection happens underneath it.
- **The finished file appears atomically.** Bytes go to `<destination>.part`,
  and a rename publishes the file on success. A crash never leaves a truncated
  file that looks finished.

---

## 3. Core concepts

The API is small once you know its rules, because the same five rules apply
everywhere. Learn them here and every facet becomes predictable.

### 3.1 Ten facets, one placement rule

`Soulseek` is ten facets and `close()`:

| Facet | Owns |
|---|---|
| `connection()` | The connection to the server |
| `search()` | Searching the network |
| `downloads()` | Downloads, and the queue that runs them |
| `uploads()` | Uploads peers request from us |
| `users()` | Reading about other users |
| `rooms()` | Chat rooms |
| `chat()` | Private messages |
| `shares()` | What we offer to the network |
| `me()` | This account |
| `diagnostics()` | What the library is doing |

A method lives on the facet that owns **the state it changes**, not the noun it
mentions. `ban(user)` names a user but changes our upload policy, so it is on
`uploads()`. `giftPrivileges(user)` names a user but spends our privilege
balance, so it is on `me()`. `watch(user)` reads about a user, so it is on
`users()`. Apply this rule and you can guess where a method is without
searching.

`close()` is idempotent and never throws. It closes every connection, listener,
timer, and transfer the client owns.

### 3.2 Blocking calls, virtual threads, interruption

Every operation blocks until it has an answer. There are no futures, callbacks
for results, async variants, or library-specific cancellation types. Run
concurrent work on virtual threads:

```java
Thread.startVirtualThread(() -> slsk.search().run(query));
```

**Cancellation is thread interruption** — the same contract as
`BlockingQueue.take()`. Every blocking method declares
`throws InterruptedException`; interrupting the calling thread cancels that
invocation. Because it is the platform's own contract, everything the JDK
gives you composes for free:

```java
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
Future<SearchResult> search = executor.submit(() -> slsk.search().run(query));
// later, from any thread:
search.cancel(true);        // interrupts the runner; run() stops the search and throws
// or: executor.shutdownNow()  // cancels every in-flight operation at once
```

**Deadlines are built in.** Every blocking method has exactly one sibling
overload taking a `java.time.Duration`, which throws
`java.util.concurrent.TimeoutException` when the deadline expires:

```java
SearchResult result = slsk.search().run(query, Duration.ofSeconds(20));
Download done = slsk.downloads().await(id, Duration.ofMinutes(10));
```

The deadline covers the whole invocation from method entry, on a monotonic
clock. It is distinct from the configured domain timeouts (section 4):
whichever fires first wins, and each throws its own exception, so "my
deadline expired" and "the server never answered" stay distinguishable.

Four rules make the semantics predictable:

- **Interruption cancels the invocation, never independently owned work.**
  Interrupting `downloads().await(id)` stops the wait; the download keeps
  running (`cancel(id)` cancels it). Interrupting `search().await(id)` stops
  the wait; the search keeps running (`stop(id)` stops it). Interrupting
  `search().run(query)` stops the search too — that invocation created and
  owns it.
- **A result committed before cancellation wins.** The call returns normally
  and the interrupt is left on the thread's flag for your own code to
  observe. The library consumes an interrupt only when it reports it as an
  `InterruptedException`, and never re-asserts one it reports.
- **Cancellation is prompt and bounded.** Cleanup on the cancellation path
  has a budget; a second interrupt during cleanup exits immediately.
- **Cancelling a send never kills a shared connection.** For methods that
  deliver a message (`chat().send`, `rooms().say`, ...), an interrupt before
  the message reaches the socket withdraws it — nothing is sent. Once the
  frame write has started, the connection is preserved and the frame
  completes; the caller gets its `InterruptedException` and delivery is
  indeterminate. A normal return means the complete message was handed to
  the local socket — not that the peer received it.

Your own threads never do socket I/O: the library keeps all socket reads and
writes on threads it owns, which is what makes interruption safe here. Do not
try to cancel library work by any other means (closing streams, reflection on
internals); interrupt the thread or use the id-based commands.

### 3.3 Ids and snapshots

The library returns two kinds of values: **ids** (`TransferId`, `SearchId`) and
**immutable snapshots** (`Download`, `SearchSnapshot`, `Room`, ...). There are
no live stateful objects to hold. An id survives being serialized to JSON,
held across an HTTP round trip, and handed back. Every command takes an id:
`pause(id)`, `cancel(id)`, `stop(id)`.

Two things follow:

- A `TransferId` identifies one **enqueue**, not one file. Enqueue the same
  file twice and you get two ids and two independent transfers.
- Both id values are opaque strings. Do not parse them.

Snapshots are records. Every collection inside one is defensively copied and
unmodifiable. A snapshot never changes after you receive it. Ask the facet
again for newer state.

### 3.4 Events: deltas on state you can always read

Each facet answers "what is true now?" synchronously and cheaply (`state()`,
`all()`, `get(id)`), and publishes **events as deltas** on that state through
one `EventStream<T>` per facet. Consequences:

- An application that misses every event and polls instead is degraded, not
  broken.
- An application starting cold never needs event history. There is no replay,
  and none is needed.
- The library keeps **no history**: no chat scrollback, no past searches, no
  completed-transfer archive. History belongs to the application.

```java
// All events on a stream:
Subscription sub = slsk.downloads().events().subscribe(event -> render(event));

// One concrete type only:
Subscription sub2 = slsk.downloads().events()
        .subscribe(DownloadEvent.Progressed.class, e -> renderProgress(e));

sub.close(); // idempotent, never throws
```

Every event type is a record in a **sealed** hierarchy rooted at
`SoulseekEvent`, and every event carries `at()` (an `Instant`). Handle a stream
with one `switch` and the compiler tells you when a new event type appears.

**A listener that throws is contained.** The exception is reported on the
diagnostics stream at `WARNING` level, and the remaining listeners still run.
It never reaches the library's read loops. The single exception to this rule is
private-message acknowledgement (see [5.7](#57-chat)).

**`attach` removes the initial-state race.** Reading state and then subscribing
can lose an event that fires in the gap. Facets with meaningful list state
offer `attach(listener)`, which captures the state and registers the listener
under one lock:

```java
try (Attachment<List<Download>> attached = slsk.downloads().attach(sse::broadcast)) {
    renderInitial(attached.state()); // consistent with the stream, exactly
    awaitShutdown();
}
```

`Attachment<S>` is a record of `state()` and `subscription()`. Closing it
closes the subscription. `connection()`, `search()`, `downloads()`,
`uploads()`, and `rooms()` have `attach`. The other facets carry no list state
worth attaching to.

### 3.5 Idempotent intents

Every command is an intent named for its goal, and asking twice does nothing:
`pause(id)` on a paused download does nothing, `cancel(id)` on a finished
transfer does nothing, `join(room)` when already joined returns the current
state. There are no toggles. This exists because the caller is often an HTTP
handler that cannot know whether its previous request arrived.

### 3.6 Outcomes are values, exceptions are faults

A peer refusing a file is normal on this network. It is returned as a value
(`TransferOutcome.Rejected`), not thrown. A search that finds nothing returns
an empty list. A user who is offline is `UserPresence.OFFLINE`, not an
exception.

Exceptions mean **faults**: not connected, protocol violation, I/O failure,
invalid argument. All library exceptions are unchecked and extend
`SoulseekClientException` (see [section 8](#8-exceptions)). Three standard
exceptions also appear: `IllegalArgumentException` for a bad argument or an
unknown id, and the two checked cancellation exceptions from
[3.2](#32-blocking-calls-virtual-threads-interruption) —
`InterruptedException` on every blocking method and `TimeoutException` on
`Duration` overloads.

---

## 4. Building a client: `SoulseekBuilder`

`Soulseek.builder()` is the only way to get a client. Two settings are
required. Everything else has a working default.

```java
Soulseek slsk = Soulseek.builder()
        .credentials("username", "password")     // required
        .applicationMinorVersion(1234)           // required, must be > 100
        .listenPort(2234)
        .share(Path.of("/music"))
        .downloads(DownloadPolicy.defaults().maxConcurrent(5))
        .uploads(UploadPolicy.standard(2, 1))
        .transferStore(TransferStore.inMemory())
        .profile(UserProfile.of("Running Soulseek.jvm"))
        .diagnostics(DiagnosticLevel.INFO)
        .peerTimeout(Duration.ofSeconds(60))
        .transferTimeout(Duration.ofSeconds(60))
        .messageTimeout(Duration.ofSeconds(10))
        .build();
```

| Method | Default | Meaning and constraints |
|---|---|---|
| `credentials(String user, String secret)` | — (required) | The account to log in as. |
| `applicationMinorVersion(int)` | — (required) | Identifies this client build to the server. Must be greater than 100 and unique to your build. `build()` fails without it. |
| `listenPort(int)` | `2234` | The port peers connect to us on. Range 1024–65535. |
| `shares(List<SharedFolder>)` | empty | The folders to share, replacing any set before. |
| `share(Path)` | — | Adds one unlocked shared folder. |
| `downloads(DownloadPolicy)` | `DownloadPolicy.defaults()` | How the download queue runs. See [5.3](#53-downloads). |
| `uploads(UploadPolicy)` | `UploadPolicy.standard(2, 1)` | Who we serve and in what order. See [6.4](#64-uploadpolicy). |
| `transferStore(TransferStore)` | `TransferStore.inMemory()` | Where the download queue survives a restart. See [6.2](#62-transferstore). |
| `catalog(ShareCatalog)` | built-in index | Serves browses, searches, and uploads from your own catalog. See [6.3](#63-sharecatalog-and-resolvedfile). |
| `profile(UserProfile)` | `UserProfile.empty()` | What peers see when they ask about this account. |
| `diagnostics(DiagnosticLevel)` | `INFO` | How much the library reports on the diagnostics stream. |
| `peerTimeout(Duration)` | 60 s | How long a peer connection may sit idle before it is dropped. Also bounds the wait for a peer's transfer acknowledgement. Must be positive. |
| `transferTimeout(Duration)` | 60 s | How long an established transfer may move no bytes before it is dropped. Generous on purpose: a congested uploader can stall for a long time and still be working. Must be positive. |
| `messageTimeout(Duration)` | 10 s | How long to wait for a server response before giving up on it. Must be positive. |
| `build()` | — | Builds the client. Does not connect. |

Setter validation throws `IllegalArgumentException` immediately. Missing
required settings make `build()` throw `IllegalStateException`.

---

## 5. Facet reference

Conventions used below:

- **Blocks** means the method can wait — for a remote answer, or for a
  message it sends to be handed to the socket. It declares
  `throws InterruptedException`, and it has one sibling overload with a
  trailing `Duration` deadline that additionally throws `TimeoutException`.
  The tables list the no-deadline form; the `Duration` form always exists
  and is never listed separately.
- **Cheap** means the method returns from memory, synchronously, and has no
  `Duration` overload.
- All value types are immutable records unless stated otherwise.

### 5.1 `connection()`

The connection to the server. One client outlives every socket it opens:
reconnection, re-subscribing watched users, and re-announcing shares all happen
underneath. The application never rebuilds the client to reconnect.

| Method | Kind | Behaviour |
|---|---|---|
| `connect()` | Blocks | Connects to the public server (`vps.slsknet.org:2271`) and logs in. Returns when online. Throws on failure (`ConnectionException`, `LoginRejectedException`, ...). A transient failure also arms the automatic reconnect, so a failed startup connect recovers on its own. The `Duration` form races the caller's deadline against the configured connect timeout; whichever fires first wins, each with its own exception. |
| `connect(ServerAddress)` | Blocks | Same, against a named server. |
| `disconnect(String reason)` | Cheap | Disconnects and stops reconnecting. Idempotent. The reason is recorded in diagnostics. |
| `state()` | Cheap | The current `ConnectionState`. |
| `server()` | Cheap | `Optional<ServerInfo>` — what the server has said about itself. Empty when not logged in. |
| `ping()` | Blocks | Measures the round trip to the server. Returns a `Duration`. |
| `events()` | Cheap | `EventStream<ConnectionEvent>`. |
| `attach(Consumer<ConnectionEvent>)` | Cheap | `Attachment<ConnectionState>`: the state and a subscription, taken atomically. |

**`ConnectionState`** is a sealed interface. Render it with one exhaustive
`switch`:

| State | Data | Meaning |
|---|---|---|
| `Offline` | — | Not connected, and not trying to be. |
| `Connecting` | `int attempt` (from 1) | Opening the socket. |
| `Authenticating` | — | Socket open, logging in. |
| `Online` | `Instant since`, `ServerInfo server` | Connected and logged in. `isOnline()` is true only here. |
| `Disconnecting` | — | Closing at our own request. |
| `Reconnecting` | `int attempt`, `Instant nextAttemptAt`, `Throwable lastFailure` | Dropped, waiting to retry. Reconnection is automatic and always on. It stops only for a rejected login, an explicit `connect`/`disconnect`, and `close()`. `nextAttemptAt` lets a UI count down and offer a retry button. |
| `Rejected` | `String reason` | Terminal. The server refused the credentials. Retrying will not help, and the library will not retry. |

`Rejected` is deliberately not a kind of `Reconnecting`: a wrong password does
not become right by waiting.

**`ServerAddress`**: `host` + `port` record. `ServerAddress.soulseek()` is the
public server. `ServerAddress.of(host, port)` for anything else.

**`ServerInfo`**: every field optional, because the server sends these as
separate messages at its own pace. `parentMinSpeed()`, `parentSpeedRatio()`
(`OptionalInt`), `wishlistInterval()` (`Optional<Duration>`), `supporter()`
(`Optional<Boolean>`). `ServerInfo.empty()` is the state before the server has
said anything.

**`ConnectionEvent`** (sealed):

| Event | Data | Meaning |
|---|---|---|
| `StateChanged` | `from`, `to` | The connection moved between states. This is the one event to drive a status indicator from. |
| `ServerInfoReceived` | `info` | The server said something about itself. Carries everything said so far, not only the new part. |
| `KickedFromServer` | `reason` | The server disconnected us, usually because the same account logged in elsewhere. |
| `GlobalMessageReceived` | `message` | A server-wide announcement. |
| `ExcludedSearchPhrasesReceived` | `List<String> phrases` | Terms the server will not accept in a search. |

### 5.2 `search()`

Searching the network. Three ways to run one, because callers want three
different things:

- `run` blocks and returns everything — what a script wants.
- `start` returns an id immediately, and results arrive as events — what a UI
  wants.
- `await` bridges the two, for a caller that started a search and later decided
  to wait.

| Method | Kind | Behaviour |
|---|---|---|
| `start(SearchQuery)` | Cheap | Starts a search. Returns a `SearchId` immediately. |
| `run(SearchQuery)` | Blocks | Starts and waits. Returns a `SearchResult`. The common case. Interrupting stops the search this call owns, then throws — partial results are discarded with the exception. |
| `await(SearchId)` | Blocks | Waits for a running search to stop. Interrupting stops **only the wait** — the search keeps running. (Changed from 1.x, where cancelling the wait stopped the search.) |
| `stop(SearchId)` | Cheap | Stops a search early. Idempotent. To stop early *and keep what arrived*: `stop(id)`, then `await(id)` — the wait returns normally because the search ended. |
| `get(SearchId)` | Cheap | The `SearchSnapshot` as it stands. Throws `IllegalArgumentException` for an unknown id. |
| `active()` | Cheap | Every search still running. |
| `events()` | Cheap | `EventStream<SearchEvent>`. |
| `attach(Consumer<SearchEvent>)` | Cheap | `Attachment<List<SearchSnapshot>>` of the running searches. |

**Retention.** Running searches are never dropped. Finished searches are kept
until one hundred more recent ones exist, then forgotten — `get` and `await`
then throw for them. An application that wants a longer history keeps the
`SearchResult` it received, which is immutable and complete.

**No ranking.** The library does not group, deduplicate, rank, or sort
responses. Those are presentation decisions that every application makes
differently. Responses arrive in arrival order.

**`SearchQuery`** — what to ask, built fluently:

```java
SearchQuery query = SearchQuery.of("miles davis kind of blue")
        .withScope(SearchScope.network())
        .withLimits(SearchLimits.defaults())
        .withFilters(new SearchFilters(
                OptionalInt.of(320),        // minBitrate (kbit/s)
                OptionalLong.empty(),       // minSize (bytes)
                OptionalLong.empty(),       // maxSize (bytes)
                true,                       // excludeLocked
                Set.of("flac", "mp3")));    // requiredExtensions (lowercase, no dot)
```

| Type | Fields / factories | Notes |
|---|---|---|
| `SearchQuery` | `terms`, `scope`, `limits`, `filters`. Factory `of(terms)`; wither per field. | `terms` must not be blank. |
| `SearchScope` | `network()` (default), `wishlist()`, `room(name)`, `users(Username...)` | A room-scoped search names exactly one room — the wire allows no more. `wishlist()` runs on the server's own interval. |
| `SearchLimits` | `overall` (15 s), `idle` (4 s), `maxResponses` (250), `maxFilesPerUser` (500). Factory `defaults()`. | The idle timeout is the load-bearing one. Soulseek never signals completion, so the search ends when responses stop arriving for `idle`, or at `overall`, whichever comes first. |
| `SearchFilters` | `minBitrate`, `minSize`, `maxSize`, `excludeLocked`, `requiredExtensions`. Factory `none()`. Method `accepts(file, locked)`. | Applied as responses arrive. A filtered-out file never reaches a listener and never counts toward the limits. |

**Result types:**

| Type | Fields | Notes |
|---|---|---|
| `SearchResult` | `id`, `query`, `status`, `responses`, `elapsed`. Helpers `isEmpty()`, `fileCount()`. | A finished search. Empty is normal, not an error. |
| `SearchSnapshot` | `id`, `query`, `status`, `startedAt`, `endedAt` (`Optional`), `responses`, `revision`. Helpers `responseCount()`, `fileCount()`. | A search as it stands. `revision` increases on every change, so a poller can compare one number instead of diffing lists. |
| `SearchStatus` | `IN_PROGRESS`, `COMPLETED`, `CANCELLED`, `TIMED_OUT`. Helper `isTerminal()`. | Every terminal status is library policy — the network never says "done". |
| `SearchResponse` | `user`, `freeUploadSlots`, `uploadSpeed` (bytes/s), `queueLength`, `files`, `lockedFiles`. Helpers `fileCount()`, `hasFreeSlot()`. | One peer's answer. `lockedFiles` are held for privileged users. |
| `SearchFile` | `path` (backslash-joined remote path), `size` (bytes), `attributes`. Helpers `name()`, `extension()`. | A file on offer. |
| `FileAttributes` | `raw` (`Map<FileAttributeType, Integer>`). Accessors `bitrate()`, `duration()`, `variableBitRate()`, `sampleRate()`, `bitDepth()`. Factories `none()` and `probe(Path)`. | Typed accessors over the raw protocol map. The raw map is kept because clients disagree about what they send. `probe` reads a local audio file's headers (MP3, FLAC, WAV, Ogg Vorbis, Opus, MP4/AAC/ALAC) so a `ShareCatalog` can publish real attributes; call it from your scanner, once per file, and persist the result. |
| `FileAttributeType` | `BIT_RATE`(0), `LENGTH`(1), `VARIABLE_BIT_RATE`(2), `SAMPLE_RATE`(4), `BIT_DEPTH`(5). `code()`, `fromCode(int)`. | Wire codes. Not contiguous. `fromCode` returns `null` for an unmodeled code. |

**`SearchEvent`** (sealed). The first two concern our searches. The last three
concern searches **other** peers ran that we matched and answered — the other
half of being on a distributed network:

| Event | Data | Meaning |
|---|---|---|
| `ResponsesReceived` | `id`, `List<SearchResponse> responses`, `revision` | Peers answered one of our searches. Already filtered. Usually one response per event today; the list allows future batching without a type change. |
| `StatusChanged` | `id`, `from`, `to` | One of our searches started or stopped. |
| `RequestReceived` | `user`, `terms`, `token` | Another peer's search reached us. |
| `ResponseDelivered` | `user`, `token`, `fileCount` | We answered another peer's search. |
| `ResponseDeliveryFailed` | `user`, `token`, `cause` | We matched but could not reach the peer. |

### 5.3 `downloads()`

Downloads, and the queue that runs them. **Enqueueing does not start
anything.** The request joins the library's queue, and the queue decides when
it runs, how many run at once (overall and per peer), and when a failed attempt
is retried.

| Method | Kind | Behaviour |
|---|---|---|
| `enqueue(DownloadRequest)` | Cheap | Enqueues. Returns the `TransferId` of this enqueue. |
| `enqueueAll(List<DownloadRequest>)` | Cheap | Enqueues several. Returns ids in the order given. |
| `pause(TransferId)` | Cheap | Stops a download and leaves it queued. Idempotent. |
| `resume(TransferId)` | Cheap | Puts a paused download back in the queue. Idempotent. |
| `retry(TransferId)` | Cheap | Puts a **finished** download back in the queue with its attempt count reset. No-op on an unfinished one. |
| `cancel(TransferId)` | Cheap | Cancels. Idempotent, no-op on a finished one. |
| `forget(TransferId)` | Cheap | Drops a finished download from the list. Throws `IllegalStateException` if it has not finished. |
| `prioritize(TransferId, Priority)` | Cheap | Moves a download within **our own** queue. Says nothing to the peer. |
| `await(TransferId)` | Blocks | Waits for a terminal state. Returns the final `Download`. Interrupting stops the wait, not the download — `cancel(id)` cancels the download. |
| `get(TransferId)` | Cheap | The `Download` snapshot. Throws `IllegalArgumentException` for an unknown id. |
| `find(TransferId)` | Cheap | `Optional<Download>`. |
| `all()` | Cheap | Every download the library holds. |
| `policy()` / `policy(DownloadPolicy)` | Cheap | Reads / replaces the queue policy, with immediate effect. |
| `events()` | Cheap | `EventStream<DownloadEvent>`. |
| `attach(Consumer<DownloadEvent>)` | Cheap | `Attachment<List<Download>>`. |

**`DownloadRequest`** — what to fetch:

```java
// Simple: to a local file.
DownloadRequest.of(user, remotePath, Path.of("downloads/track.flac"));

// From a search result (carries the advertised size):
DownloadRequest.of(response.user(), searchFile, destination);

// Full control:
DownloadRequest request = DownloadRequest.builder(user, remotePath, destination)
        .expectedSize(12_345_678L)
        .priority(Priority.HIGH)
        .tag("albumId", "42")     // handed back on every event for this transfer
        .build();
```

Fields: `user`, `path`, `sink` (`TransferSink` — where the bytes go, see
[6.1](#61-transfersink)), `expectedSize` (0 = unknown), `priority`, `tags`
(`Map<String, String>`). Factories also accept a `TransferSink` in place of a
`Path`.

**Tags matter for UIs.** Without them, an application that wants to know which
album a file belongs to keeps a side table keyed on `(user, path)` — which goes
stale the moment the same path is enqueued again. Attach the data at enqueue
time and receive it back on every snapshot and event.

**`Download`** — the snapshot: `id`, `user`, `path`, `size`, `state`,
`priority`, `enqueuedAt`, `startedAt` (`Optional`), `endedAt` (`Optional`),
`attempt` (from 1), `tags`. Helpers `name()`, `isFinished()`.

**`TransferState`** (sealed) — where a transfer is right now. Each state
carries exactly the data that state has:

| State | Data | Meaning |
|---|---|---|
| `Queued` | `int localPosition` (from 0) | In our own queue. We have not asked the peer yet. We control this queue and can reorder it. |
| `Requesting` | — | We asked the peer and are waiting for its answer. |
| `QueuedRemotely` | `OptionalInt position`, `Instant polledAt` | In the peer's queue. We can only wait. The position is present only if the peer answered a poll — not all do. |
| `Connecting` | `boolean indirect` | Establishing the transfer connection. `indirect` means we are going through the server because a direct connection failed — slower, and worth showing. |
| `Transferring` | `Progress progress` | Bytes are moving. |
| `Paused` | `TransferState resumeTo` | Held by the application. `resumeTo` is the state resuming returns to. |
| `Finished` | `TransferOutcome outcome` | Over, one way or another. |

Helpers: `isTerminal()` (true only for `Finished`), `isActive()` (connecting or
transferring). The `Queued` / `QueuedRemotely` distinction is real to a user:
one queue is ours, the other belongs to the peer.

**`TransferOutcome`** (sealed) — how it ended. A value, not an exception:

| Outcome | Data | Meaning |
|---|---|---|
| `Succeeded` | `long bytes`, `Duration elapsed` | Complete. |
| `Cancelled` | — | We cancelled. Not a failure, never retried. |
| `Rejected` | `RejectionReason reason`, `String rawMessage` | The peer refused. `rawMessage` is verbatim, for display when `reason` is `UNKNOWN`. |
| `Failed` | `Throwable cause`, `boolean retryable` | A fault on our side or on the wire. Timeouts and drops are retryable, malformed messages are not. |

Helper: `isSuccess()`.

**`RejectionReason`** — Soulseek carries rejections as free text. The library
string-matches once so applications do not: `FILE_NOT_SHARED`, `BANNED`,
`TOO_MANY_FILES`, `TOO_MANY_MEGABYTES`, `PENDING_SHUTDOWN`, `QUEUE_FULL`,
`CANCELLED_BY_PEER`, `UNKNOWN`. Adding a constant is not a breaking change: a
correct consumer already handles `UNKNOWN`, and the raw text is always kept.

**`Progress`** — `transferred`, `total` (0 = unknown), `bytesPerSecond`
(smoothed by the library, once), `eta` (`Optional<Duration>`, absent when no
estimate exists). Helpers `fraction()` (0.0–1.0, 0.0 when size unknown),
`isComplete()`. Progress is emitted on a fixed cadence, not per socket read.

**`Priority`** — `LOW`, `NORMAL`, `HIGH`. Orders work **we** have not started
yet. It says nothing to a peer, and no client can jump a remote queue.

**`DownloadPolicy`** — how the queue runs. Immutable, with a wither per field:

| Field | Default | Meaning |
|---|---|---|
| `maxConcurrent` | 3 | Downloads transferring at once, across all peers. |
| `maxConcurrentPerUser` | 1 | At once from any one peer. **Keep this at 1 unless you know better**: opening four connections to one peer looks like an attack from their side, and their client treats it as one. Must not exceed `maxConcurrent`. |
| `speedLimit` | `Bandwidth.unlimited()` | Aggregate download rate ceiling. |
| `queuePositionPollInterval` | 30 s | How often to ask each peer where we sit in its queue. |
| `retry` | `RetryPolicy.defaults()` | When a failed download is retried. |

These bound **transfers, not queueing**. Wanting a file costs one line in the
peer's queue and opens nothing, so every track of an album is asked for at
once. The concurrency caps apply only once a peer says it is ready. The peer,
not this policy, decides when a download starts.

**`RetryPolicy`** — `maxAttempts` (3), `initialBackoff` (5 s), `multiplier`
(2.0), `maxBackoff` (5 min), `retryableRejections`. The default retries
exactly two rejection reasons: `QUEUE_FULL` and `PENDING_SHUTDOWN` — refusals
about this moment, not this file. Retrying `FILE_NOT_SHARED` or `BANNED` earns
a ban. `RetryPolicy.none()` never retries. Methods `backoffBefore(attempt)`
and `shouldRetry(outcome, attempt)` expose the logic for testing. Failures
(`TransferOutcome.Failed`) retry when their `retryable` flag is true.

**`Bandwidth`** — a rate limit with a named unit: `unlimited()`,
`ofBytesPerSecond(long)`, `ofKibibytesPerSecond(double)`,
`ofMegabitsPerSecond(double)`. `isUnlimited()` is true at 0.

**`DownloadEvent`** (sealed):

| Event | Data | Meaning |
|---|---|---|
| `Enqueued` | `Download download` | Joined the queue. |
| `StateChanged` | `id`, `from`, `to` | Moved between states. |
| `Progressed` | `id`, `progress` | Bytes moved. Coalesced to a fixed cadence, rate already smoothed. |
| `QueuePositionChanged` | `id`, `OptionalInt position` | Our place in the peer's queue changed or was first reported. |
| `Finished` | `id`, `outcome` | Ended. **Read the terminal outcome from this event** — see [section 9](#9-building-a-ui-on-this-api). |
| `RetryScheduled` | `id`, `attempt`, `nextAttemptAt` | A failed download will be tried again. |
| `Forgotten` | `id` | A finished download was dropped from the list. |

### 5.4 `uploads()`

Uploads peers have asked us for. There is no `start` here: peers ask, and the
configured `UploadPolicy` answers ([6.4](#64-uploadpolicy)). What is ours is
cancelling, reordering our own queue, and deciding who we serve.

| Method | Kind | Behaviour |
|---|---|---|
| `all()` | Cheap | Every upload the library holds. |
| `get(TransferId)` | Cheap | The `Upload` snapshot. Throws `IllegalArgumentException` for an unknown id. |
| `find(TransferId)` | Cheap | `Optional<Upload>`. |
| `cancel(TransferId)` | Cheap | Stops an upload. Idempotent. The peer is told, so their client stops waiting instead of timing out. |
| `prioritize(TransferId, Priority)` | Cheap | Moves a **queued** upload within our queue. Does nothing to one already running. |
| `policy()` / `policy(UploadPolicy)` | Cheap | Reads / replaces the upload policy. |
| `ban(Username, String reason)` | Cheap | Refuses to serve a user from now on. Idempotent. The reason is recorded and sent to them when they ask. |
| `unban(Username)` | Cheap | Serves the user again. Idempotent. |
| `banned()` | Cheap | `Map<Username, String>` — who we refuse, and why. |
| `events()` | Cheap | `EventStream<UploadEvent>`. |
| `attach(Consumer<UploadEvent>)` | Cheap | `Attachment<List<Upload>>`. |

**Selection order.** When a slot frees, the next upload is drawn by a hard gate
per tier: server-privileged users first, then `HIGH`, `NORMAL`, `LOW`.
Within a tier, peers are served round-robin, and one peer's requests are FIFO.
Privileged precedence is protocol-mandated, not a matter of taste.

**There is no buddy list, deliberately.** Who counts as a favoured peer is the
application's knowledge. Express it by raising that peer's queued uploads to
`Priority.HIGH`.

**`Upload`** — the snapshot: `id`, `user`, `path`, `size`, `state`,
`priority`, `requestedAt`, `startedAt` (`Optional`), `endedAt` (`Optional`).
Helpers `name()`, `isFinished()`. No tags: we did not enqueue it, so there was
nothing to attach.

**`UploadEvent`** (sealed):

| Event | Data | Meaning |
|---|---|---|
| `Requested` | `Upload upload` | A peer asked, and the policy accepted (allow or queue). |
| `StateChanged` | `id`, `from`, `to` | Moved between states. |
| `Progressed` | `id`, `progress` | Bytes moved. Coalesced. |
| `Finished` | `id`, `outcome` | Ended. Read the terminal outcome here. |
| `Denied` | `user`, `path`, `reason` | We refused a peer, by policy or by ban. |

### 5.5 `users()`

Reading about other users. These are questions with answers, so they block and
return values. Offline is an answer, not an exception. Nothing that changes
*our* state lives here (banning is on `uploads()`, gifting is on `me()`).

| Method | Kind | Behaviour |
|---|---|---|
| `info(Username)` | Blocks | Asks the user to describe themselves. Returns `UserInfo`. |
| `statistics(Username)` | Blocks | Asks the server for sharing figures. Returns `UserStatistics`. |
| `status(Username)` | Blocks | Asks the server whether the user is around. Returns `UserStatus`. |
| `endpoint(Username)` | Blocks | Resolves the address to connect to a user on. Cached by the library, so usually free. Returns `InetSocketAddress`. |
| `browse(BrowseRequest)` | Blocks | Reads everything a user shares. Returns `Browse`. |
| `directory(Username, String path)` | Blocks | Reads one directory of a user's share. Returns `List<Directory>` — the protocol answers with a list, and a peer may include subdirectories. Empty if none. |
| `watch(Username)` | Cheap | Opens a status subscription. Returns a `Watch`. |
| `watched()` | Cheap | `Set<Username>` currently watched. |
| `events()` | Cheap | `EventStream<UserEvent>`. |

**Browsing.** A browse is one message, and a well-shared account's browse is
megabytes — two hundred thousand entries is ordinary. The request therefore
carries an optional progress callback:

```java
Browse browse = slsk.users().browse(
        BrowseRequest.of(user)
                .timeout(Duration.ofSeconds(120))
                .onProgress(p -> bar.set(p.fraction())));
```

| Type | Fields / methods | Notes |
|---|---|---|
| `BrowseRequest` | `user`, `timeout` (default 60 s — the wait for the peer to *start* responding), `onProgress` (`Optional<Consumer<BrowseProgress>>`). Factory `of(user)`; wither per field. | The request carries no cancellation handle — a request is a reusable value, and cancellation concerns one execution: interrupt the calling thread, or use `browse(request, Duration)` for a whole-call deadline. The request's `timeout` is the domain wait for the peer to start responding; the two compose. |
| `BrowseProgress` | `user`, `transferred`, `total`. Helper `fraction()`. | The peer declares the total up front. |
| `Browse` | `user`, `at`, `directories`, `lockedDirectories`. Helpers `directoryCount()`, `fileCount()`, `totalBytes()`, `at(path)` (`Optional<Directory>`), `files()` (lazy `Stream<SearchFile>`). | **Flat, not a tree.** The protocol has no directory tree: a browse is a flat list of directories, each carrying its full backslash-joined path. Build the tree your own display needs. `files()` is lazy so scanning for one extension does not materialize everything. |
| `Directory` | `name` (full remote path), `files` (`List<SearchFile>`). Helpers `fileCount()`, `simpleName()`. Factory `of(name)`. | |

**User description types:**

| Type | Fields | Notes |
|---|---|---|
| `UserInfo` | `user`, `description`, `picture` (`Optional<byte[]>`), `uploadSlots`, `queueLength`, `hasFreeUploadSlot` | Peer-supplied. None of it is trustworthy, and the picture is arbitrary bytes from a stranger. Treat as display data. |
| `UserStatistics` | `user`, `averageSpeed` (bytes/s), `uploadCount`, `fileCount`, `directoryCount`. Helper `sharesNothing()`. | What upload policies weigh. |
| `UserStatus` | `user`, `presence`, `privileged`. Helper `isOnline()`. | `privileged` users get protocol-mandated queue precedence. |
| `UserPresence` | `OFFLINE`, `AWAY`, `ONLINE` | Offline is a value, not a failure. |

**`Watch`** — an open subscription to a user's status. Soulseek's subscription
is server-side and dies with the connection, so the library re-registers every
watch on each login: a consumer holding a `Watch` keeps working across
reconnects without knowing they happened. Watches are reference-counted — two
parts of an application watching the same user share one server-side
subscription, and it drops only when the last `Watch` closes. Members:
`user()`, `status()` (as last reported), `close()` (idempotent, never throws).

**`UserEvent`** (sealed). Arrives only for users under an active watch:

| Event | Data | Meaning |
|---|---|---|
| `StatusChanged` | `user`, `from`, `to` (`UserStatus`) | A watched user came online, went away, or went offline. |
| `StatisticsChanged` | `user`, `statistics` | A watched user's sharing figures changed. |
| `CannotConnect` | `user`, `reason` | We could not reach a user we tried to connect to. |

### 5.6 `rooms()` and `privateRooms()`

Chat rooms. `get` and `joined()` answer with **state** — who is in a room,
what is pinned — never with messages. Messages are history, and history
belongs to the application (see [5.7](#57-chat) for the same rule on private
messages).

| Method | Kind | Behaviour |
|---|---|---|
| `list()` | Blocks | The server's room directory. Returns `RoomList`. |
| `join(String room)` | Blocks | Joins. Idempotent: joining a room we are in returns its current state. Returns `Room`. Throws `RoomJoinForbiddenException` when refused. |
| `leave(String room)` | Blocks | Leaves. Idempotent. Returns when the message is handed to the socket. |
| `say(String room, String message)` | Blocks | Says something in a room. Returns when the message is handed to the socket. |
| `setTicker(String room, String message)` | Blocks | Pins our ticker, replacing whatever we pinned before. Returns when handed to the socket. |
| `get(String room)` | Cheap | A room we are in. Throws `IllegalArgumentException` if we are not in it. |
| `joined()` | Cheap | Every room we are in. |
| `startPublicChat()` / `stopPublicChat()` | Blocks | Starts / stops the all-rooms message firehose. Idempotent. Returns when handed to the socket. |
| `privateRooms()` | Cheap | The private-room administration facet. |
| `events()` | Cheap | `EventStream<RoomEvent>`. |
| `attach(Consumer<RoomEvent>)` | Cheap | `Attachment<List<Room>>`. |

| Type | Fields | Notes |
|---|---|---|
| `Room` | `name`, `users` (`List<RoomUser>`), `tickers`, `isPrivate`, `owner` (`Optional<Username>`), `operators` (`Set<Username>`). Helper `userCount()`. | No message list, deliberately. |
| `RoomInfo` | `name`, `userCount` | A directory entry, before joining. |
| `RoomList` | `publicRooms`, `privateRooms` (we are a member), `owned`, `moderated`. Factory `empty()`. | The directory, split by our relationship to each room. |
| `RoomTicker` | `user`, `message` | A pinned message. One per user — a second replaces the first. |
| `RoomUser` | `user`, `status` (`UserPresence`), `statistics`, `freeUploadSlots` (`OptionalInt`), `countryCode` (`Optional<String>`) | The server sends everyone's figures on join, so no per-user lookups are needed. |

**`PrivateRooms`** — administration of rooms we own or moderate. Every method
blocks briefly (each has the standard `Duration` overload) and is an
idempotent intent (adding an existing member does nothing):

| Method | Behaviour |
|---|---|
| `addMember(room, user)` | Adds a member to a room we own. |
| `removeMember(room, user)` | Removes a member. |
| `addOperator(room, user)` | Makes a member a moderator. |
| `removeOperator(room, user)` | Removes a moderator. |
| `dropMembership(room)` | Gives up our membership. |
| `dropOwnership(room)` | Gives up our ownership. |

**`RoomEvent`** (sealed):

| Event | Data | Meaning |
|---|---|---|
| `Joined` | `room`, `state` (`Room`) | We joined. |
| `Left` | `room` | We left. |
| `MessageReceived` | `room`, `from`, `message` | Somebody spoke in a room we are in. The one event that is not a state delta — store it or lose it. |
| `UserJoined` | `room`, `user` (`RoomUser`) | Somebody joined. |
| `UserLeft` | `room`, `user` | Somebody left. |
| `TickerAdded` | `room`, `ticker` | Somebody pinned a ticker. |
| `TickerRemoved` | `room`, `user` | Somebody removed theirs. |
| `TickerListReceived` | `room`, `tickers` | The server replaced the whole ticker list. |
| `ListReceived` | `list` (`RoomList`) | The server sent its directory. |
| `PublicChatMessageReceived` | `room`, `from`, `message` | Firehose message for a room we are not in. |
| `MembershipAdded` / `MembershipRemoved` | `room` | We were added to / removed from a private room. |
| `ModerationAdded` / `ModerationRemoved` | `room` | We were made / stopped being a moderator. |

### 5.7 `chat()`

Private messages. Two members, because there is little to this: send one, and
receive them.

| Method | Kind | Behaviour |
|---|---|---|
| `send(Username to, String message)` | Blocks | Sends a private message. Returns when the message is handed to the socket. Interrupting before the write starts withdraws the message; after, delivery is indeterminate and the connection is unaffected. |
| `events()` | Cheap | `EventStream<ChatEvent>`. |

There is no scrollback and no way to ask for it. A message is history the
instant it arrives, and only the application knows how much history to keep
and where.

**Acknowledgement is automatic and load-bearing.** The library acknowledges a
message to the server once at least one listener received it without throwing.
If nobody is listening, or every listener throws, the message stays
unacknowledged and the server delivers it again at the next login. This is the
one place a listener exception is observed rather than merely reported —
register your chat listener before connecting if you must not miss messages.

**`ChatEvent`** (sealed) has exactly one member:

| Event | Data | Meaning |
|---|---|---|
| `MessageReceived` | `from`, `message`, `wasReplayed`, `sentAt`, `at` | Somebody sent us a message. `wasReplayed` is true when the server redelivers something sent while we were offline — that is the field a UI checks to decide whether to fire a notification. |

### 5.8 `shares()`

What we offer to the network.

| Method | Kind | Behaviour |
|---|---|---|
| `configure(List<SharedFolder>)` | Cheap | Sets the folders to share, replacing whatever was set. Does not scan. |
| `configured()` | Cheap | The folders currently configured. |
| `rescan()` | Blocks | Rebuilds the index and announces the counts to the server. Reports progress through `events()`. Returns the rebuilt `ShareIndex`. |
| `index()` | Cheap | The current `ShareIndex`. |
| `catalog(ShareCatalog)` | Cheap | Replaces the built-in index entirely. See [6.3](#63-sharecatalog-and-resolvedfile). |
| `events()` | Cheap | `EventStream<ShareEvent>`. |

Announcing the counts is part of a successful scan, not a separate call. This
matters: a client the server believes shares nothing is a client many peers
refuse to serve.

The built-in index holds every path in memory and matches searches by
substring. That is right for a few thousand files and wrong for a few hundred
thousand — install a `ShareCatalog` when you have a real index of your own.

| Type | Fields | Notes |
|---|---|---|
| `SharedFolder` | `path`, `locked`. Factories `of(path)`, `locked(path)`. | A locked folder's contents are reserved for privileged users. |
| `ShareIndex` | `directoryCount`, `fileCount`, `totalBytes`, `lastScan` (`Optional<Instant>`), `status`. Factory `empty()`. | The counts are what the server is told. |
| `ShareIndex.ScanStatus` | `NEVER_SCANNED`, `SCANNING`, `READY`, `FAILED` | On `FAILED`, the index is whatever it was before. |

**`ShareEvent`** (sealed):

| Event | Data | Meaning |
|---|---|---|
| `ScanStarted` | — | A scan began. |
| `ScanProgressed` | `directoriesScanned`, `filesFound` | A scan made progress. |
| `ScanCompleted` | `index` | A scan finished and the counts were announced. |
| `BrowseServed` | `to`, `fileCount` | We served a peer's browse request. |

### 5.9 `me()`

This account: who we are, and the things only we can change about ourselves.

| Method | Kind | Behaviour |
|---|---|---|
| `username()` | Cheap | The account we are logged in as. |
| `presence()` | Cheap | The presence we last published. |
| `presence(UserPresence)` | Blocks | Publishes our presence (`ONLINE` / `AWAY`). Idempotent. Returns when handed to the socket. |
| `profile()` | Cheap | What peers see when they ask about this account. |
| `profile(UserProfile)` | Cheap | Sets it. Set once, served to every peer who asks. Local: the profile is served on request, never pushed. |
| `privileges()` | Blocks | Days of privileges remaining, or zero. |
| `giftPrivileges(Username to, int days)` | Blocks | Gives some of our privilege days to another user. Returns when handed to the socket. |
| `changePassword(String)` | Blocks | Changes this account's password. Returns when the server has answered. |
| `events()` | Cheap | `EventStream<MeEvent>`. |

**`UserProfile`** — `description`, `picture` (`Optional<byte[]>`),
`uploadSlots`, `queueLength`, `hasFreeUploadSlot`. Factories `empty()` and
`of(description)`. The default is `empty()`, which answers peers with an empty
profile — silence would read as a broken client, and clients that look broken
do not get served. The slot and queue figures are what the account *claims*;
the upload policy is what makes them true.

**`MeEvent`** (sealed):

| Event | Data | Meaning |
|---|---|---|
| `LoggedIn` | `server` (`ServerInfo`) | The server accepted our credentials. Fires on every login, including automatic reconnects. |
| `PrivilegeNotificationReceived` | `from` (`Username`) | Somebody gave us privileges. Acknowledged automatically. |
| `PrivilegedUserListReceived` | `users` | The server's list of privileged users. |
| `PresenceChanged` | `from`, `to` | We changed our own presence. |

### 5.10 `diagnostics()`

What the library is doing, and where it sits on the network. Read-only, except
for `protocolTrace`, which changes only how much it says.

| Method | Kind | Behaviour |
|---|---|---|
| `events()` | Cheap | `EventStream<DiagnosticEvent>` — the library's log, including contained listener faults at `WARNING`. |
| `metrics()` | Cheap | A `Metrics` snapshot of current counters. |
| `mesh()` | Cheap | A `MeshState` snapshot of our position in the distributed search mesh. |
| `meshEvents()` | Cheap | `EventStream<MeshEvent>`. |
| `protocolTrace(boolean)` | Cheap | Turns per-message protocol tracing on or off. Expensive and very loud. Idempotent. |

**`DiagnosticEvent`** — a record, not a hierarchy: `level`
(`DiagnosticLevel`), `source` (the fully qualified emitter class name),
`message`, `exception` (`Optional<Throwable>`), `at`. Wire it to your logger
using `source` as the category, so the logging framework can filter the
library by package or class:

```java
slsk.diagnostics().events().subscribe(e ->
        org.slf4j.LoggerFactory.getLogger(e.source())
                .atLevel(map(e.level()))
                .setCause(e.exception().orElse(null))
                .log(e.message()));
```

**`DiagnosticLevel`** — `NONE`, `WARNING`, `INFO`, `DEBUG`, `TRACE`. Ordered
from silent to loudest, so a filter is a comparison.

**`Metrics`** — all-`long`/`int` counters: `bytesDownloaded`, `bytesUploaded`,
`activeDownloads`, `activeUploads`, `queuedDownloads`, `queuedUploads`,
`peerConnections`, `activeSearches`, `messagesSent`, `messagesReceived`.
Factory `empty()`. Designed for polling by a metrics exporter.

**`MeshState`** — `hasParent`, `parent` (`Optional<Username>`), `children`,
`isBranchRoot`, `branchLevel`, `branchRoot` (`Optional<Username>`). Helpers
`childCount()`, `isConnected()`. Soulseek distributes search traffic through a
tree of peers. This is a rendering surface, not a control surface: the protocol
chooses parents and children on its own terms.

**`MeshEvent`** (sealed) — one member, `StateChanged(from, to)`, because the
thing being rendered is the state, not the transitions.

---

## 6. The SPI: what you implement

`dev.slsk.spi` is the inversion: the points where the library asks the
application a question it cannot answer itself. Every SPI is blocking, and
every one has a working default. Implement none of them and you still have a
correct client.

| SPI | Question it answers | Default |
|---|---|---|
| `TransferSink` | Where do the bytes of a download go? | `TransferSink.file(path)` — write beside the destination, rename atomically. |
| `TransferStore` | Does the download queue survive a restart? | `TransferStore.inMemory()` — no. |
| `ShareCatalog` | What is this account sharing? | The built-in index over the configured folders (or `ShareCatalog.empty()` when none). |
| `UploadPolicy` | What do we do when a peer asks for a file? | `UploadPolicy.standard(2, 1)`. |

### 6.1 `TransferSink`

Where the bytes of a download go. Three calls and one guarantee:

| Method | Contract |
|---|---|
| `WritableByteChannel open(long resumeOffset)` | Opens for writing. A non-zero offset is a resume: the first byte written belongs at that position, and everything already below it is kept. Throws `IOException`. |
| `void commit()` | Makes the result visible. Called exactly once, on success. Throws `IOException`. |
| `void discard()` | Abandons this attempt. Called on failure or cancellation. **Must not throw** — it runs on a path that already failed once. |

The guarantee: nothing incomplete is ever visible at the destination.

`TransferSink.file(destination)` writes to `<destination>.part` and renames
atomically on commit (with a plain replacing move where the filesystem cannot
promise atomicity). On discard, the `.part` file is left in place — it is
exactly what a retry resumes from.

Implement this yourself to stream into object storage, hash while writing, or
decode on the fly.

### 6.2 `TransferStore`

Where the download queue survives a restart. The library owns the queue, so it
owns the problem of a queue that outlives the process — but whether a hundred
queued downloads should still be there tomorrow is your decision.

| Method | Contract |
|---|---|
| `save(Download)` | Records a download's current state, replacing any previous record. |
| `delete(TransferId)` | Forgets a download. |
| `loadAll()` | Returns every recorded download, for restoring the queue at startup. |

`TransferStore.inMemory()` is correct for a short-lived process and wrong for a
daemon. A daemon backs this with SQLite or similar and installs it via
`builder().transferStore(...)`.

### 6.3 `ShareCatalog` and `ResolvedFile`

What this account is sharing, from the point of view of a peer asking. One
interface answers all four questions so the answers cannot disagree — a browse
must not list a file that an upload would refuse to open. Every method takes
the requester, because a share may differ per peer: that is what a private
share is.

| Method | Contract |
|---|---|
| `BrowseResponse browse(Username requester)` | Everything the requester may see. |
| `List<Directory> directory(Username requester, String path)` | One directory's contents (a list — subdirectories are allowed). |
| `List<SearchFile> search(Username requester, String terms, int limit)` | Matches for a peer's search, at most `limit`. |
| `Optional<ResolvedFile> resolve(Username requester, String path)` | A file the peer wants to download, or empty when this requester may not have it. Empty is an answer, not an error. |
| `ShareIndex index()` | What the catalog holds, for the counts announced to the server. |

`ShareCatalog.empty()` shares nothing and is a correct client: browses return
empty, searches match nothing, every upload request is declined. It is called
from a peer's connection, not from a read loop, so a slow catalog delays one
peer.

**`BrowseResponse`** — `directories`, `lockedDirectories`. Factories
`empty()`, `of(directories)`. Helper `fileCount()`. Locked directories are
listed but not served — peers can see there is more. A catalog that does not
use the convention returns an empty locked list and loses nothing.

**`ResolvedFile`** — a shared file resolved to something the library can send:
`long size()` and `ReadableByteChannel open(long offset)` (the offset supports
peer resumes). `ResolvedFile.of(path)` backs one with a local file.

### 6.4 `UploadPolicy`

Who we serve, and in what order. A `@FunctionalInterface` with one method:

```java
UploadPolicy.Decision decide(UploadRequest request, UploadContext context);
```

A pure function of two values, so a policy is testable without a client.

**`UploadRequest`** (spi): `user`, `path`, `size` (0 = not known yet).

**`UploadContext`** — what is true right now:

| Method | Meaning |
|---|---|
| `requesterStatistics()` | The server's `UserStatistics` for the requester. |
| `requesterIsPrivileged()` | Whether they bought privileges. Privileged users jump queues — protocol-mandated. |
| `bytesAlreadySentTo(Username)` | Bytes sent to a user this session. |
| `activeSlots()` | Uploads running right now. |
| `queueDepth()` | Uploads waiting. |
| `activeSlotsForRequester()` | Uploads running for this requester. |

**`Decision`** (sealed):

| Decision | Data | Meaning |
|---|---|---|
| `Allow` | — | Serve it now. |
| `Queue` | `int position` (from 1) | Serve it later, and tell the peer where they stand. |
| `Deny` | `RejectionReason reason`, `String message` | Refuse, with a reason the peer's client can act on. |

Factories: `UploadPolicy.standard(slots, perUser)` — slot cap, per-user cap,
privileged users to the front of the queue. `UploadPolicy.refuseAll()` — what a
client that shares nothing says, as an answer rather than a failure.

Example — refuse peers who share nothing, otherwise standard:

```java
UploadPolicy leechGate = (request, context) -> {
    if (context.requesterStatistics().sharesNothing()) {
        return new UploadPolicy.Decision.Deny(
                RejectionReason.BANNED, "Share something first.");
    }
    return UploadPolicy.standard(2, 1).decide(request, context);
};
slsk.uploads().policy(leechGate);
```

---

## 7. Utility types

### 7.1 `Username`

A wrapped username — the key of nearly everything the network does. Exists so
that `say(room, message)` and `send(user, message)` cannot be swapped silently.

- **Case-sensitive, case-preserving.** The wire carries the exact string, and
  every correlation map is keyed on it. A name differing in case is a
  different `Username`.
- Validation is thin on purpose: rejected are only `null`, blank, and control
  characters (which would corrupt message framing). The server owns the real
  rules.
- `Username.of(value)`, `value()`, `toString()` returns the raw name,
  `Comparable` by the underlying string.

### 7.2 `RemotePath`

Static translation between local files and the backslash-joined virtual paths
Soulseek puts on the wire. A shared file is advertised as
`ShareName\relative\path.ext` — peers never learn the local layout. The
convention matches Nicotine+, including its `@@BACKSLASH@@` sentinel for a
literal backslash in a file name.

| Method | Contract |
|---|---|
| `basename(String)` | The final segment (file name). Accepts either separator. Pure string operation. |
| `parent(String)` | Everything before the final segment, or `""`. |
| `lastFolderSegment(String)` | The deepest folder name, or `""`. |
| `toRemote(String shareName, Path root, Path file)` | Builds the virtual path a local file is advertised under. Inputs are caller-controlled, so bad input **throws** (`IllegalArgumentException`, `NullPointerException`). |
| `toLocal(String remotePath, String shareName, Path root)` | Resolves a **peer-supplied** path to a local file, or rejects it. Every rejection is `Optional.empty()`, never an exception: traversal, absolute paths, wrong share, empty segments, NUL bytes, non-existent files, and — the backstop — anything whose real path resolves outside the real share root (which catches symlinks syntax cannot). |

Two rules when you use `toLocal`:

1. Answer every rejection with one identical message. Distinguishing them turns
   your reply into a filesystem oracle.
2. Containment is necessary, not sufficient. Also confirm the file is one you
   actually share.

Write your own `ShareCatalog` on top of these and you inherit the same
path-safety boundary the built-in index uses.

### 7.3 `TransferId` and `SearchId`

Opaque string ids (see [3.3](#33-ids-and-snapshots)). Both have `of(String)`
and expose `value()`. `SearchId.ofToken(int)` wraps the protocol token a
search was issued with — the string form is aligned with the wire token so
diagnostics correlate, but treat it as opaque.

---

## 8. Exceptions

All library exceptions are unchecked and extend `SoulseekClientException`
(itself a `RuntimeException`). The hierarchy:

```
SoulseekClientException
├── AddressException                 errors involving network addresses
├── ConnectionException              errors involving network connections
│   ├── ConnectionReadException      errors reading from a connection
│   ├── ConnectionWriteException     errors writing to a connection
│   ├── ConnectionWriteDroppedException  a write was dropped
│   └── ProxyException               errors involving a configured proxy
├── DownloadEnqueueException         errors while enqueueing a download
├── DuplicateTokenException          reuse of an active protocol token
├── KickedFromServerException        the server disconnected this account
├── ListenException                  errors listening for peer connections
├── LoginRejectedException           the server rejected a login
├── MessageException                 errors reading/writing protocol messages
│   ├── MessageCompressionException  errors (de)compressing messages
│   └── MessageReadException         errors reading a message
├── NoResponseException              an expected response never arrived
├── RoomException                    errors involving chat rooms
│   └── RoomJoinForbiddenException   joining a room is forbidden
├── TransferException                errors involving transfers
│   ├── DuplicateTransferException   the transfer already exists
│   ├── TransferNotFoundException    the transfer is not known
│   └── TransferRejectedException    the transfer was rejected
├── TransferReportedFailedException  the peer reported a transfer failure
├── TransferSizeMismatchException    remote size ≠ local size; carries getLocalSize() / getRemoteSize()
├── TransferStreamException          errors involving a transfer data stream
├── UserEndpointException            errors resolving a user's endpoint
│   └── UserEndpointCacheException   errors in the endpoint cache
├── UserNotFoundException            the user could not be found
└── UserOfflineException             the operation needs a user who is offline
```

Remember the division of labor from [3.6](#36-outcomes-are-values-exceptions-are-faults):
routine transfer failures reach you as `TransferOutcome` values on the
`Finished` event, not as these exceptions. The exceptions surface from
blocking calls that faulted: `connect` throws `LoginRejectedException`,
`users().info(...)` on an unreachable peer throws a `ConnectionException`
subtype or `UserOfflineException`, and so on. Catch
`SoulseekClientException` at your operation boundary and render its message.

Also thrown, from the JDK: `IllegalArgumentException` (bad argument, unknown
id in `get`), `IllegalStateException` (`forget` on an unfinished transfer,
builder misuse), `NullPointerException` (null arguments).

Two **checked** JDK exceptions carry cancellation, and only cancellation:

- `InterruptedException` — the calling thread was interrupted during a
  blocking method. The invocation was cancelled; the interrupt is consumed.
- `java.util.concurrent.TimeoutException` — the caller's own `Duration`
  deadline expired. Distinct from the configured domain timeouts, which
  throw `NoResponseException` / `ConnectionException` subtypes as listed
  above: "I gave up" and "the network gave up" are different facts and keep
  different exceptions.

---

## 9. Building a UI on this API

The API was designed against a real consumer (an HTTP/SSE server with a web
UI). These patterns are what it expects, and each closes a real bug class.

**Render from state, update from events.** For each facet: `attach` to get a
consistent starting point plus a subscription, project events onto your view
model, and re-read facet state whenever you doubt yourself. Missing an event
degrades you to a poller — it never breaks you.

```java
try (var downloads = slsk.downloads().attach(this::onDownloadEvent);
     var uploads   = slsk.uploads().attach(this::onUploadEvent);
     var conn      = slsk.connection().attach(this::onConnectionEvent)) {
    view.init(downloads.state(), uploads.state(), conn.state());
    runUntilShutdown();
}
```

**One exhaustive switch per stream.** The hierarchies are sealed so the
compiler tells you when the library adds an event. Do not write default arms
that swallow unknown events silently — let the compiler find your projections.

**Take terminal outcomes from the `Finished` event, never from a later
lookup.** The library removes a completed transfer from `all()` in the same
step that completes it. A consumer that hears `Finished` and then re-reads the
facet races that removal, and marking rows "failed" because they left the live
list loses that race visibly. The outcome is on the event. Use it.

**Progress is pre-smoothed and pre-throttled.** `Progressed` events arrive on
a fixed cadence with a smoothed rate and an honest `eta` (absent when unknown
— render nothing rather than `0:00`). Do not smooth again, and never do UI
work per byte.

**Drive the connection indicator from `ConnectionState`.** Two states carry UI
duties: `Reconnecting` gives you `nextAttemptAt` for a countdown and a working
"retry now" button (call `connect` again), and `Rejected` is terminal — show
the reason and stop, because the library will not retry a refused login.

**Commands are idempotent ids.** Wire buttons straight to `pause(id)` /
`resume(id)` / `cancel(id)` / `retry(id)`. Duplicate delivery is harmless by
contract, so an HTTP handler needs no dedup.

**History is your job.** Chat messages, room messages, finished searches
beyond the last hundred, forgotten transfers: the library holds what is true
now. Persist what your UI must show later, at the moment the event arrives.

**Presentation is your job.** Search-result grouping, "best source" ranking,
browse trees: the library hands you flat, ordered, complete data and takes no
position.

**Contain your own faults.** A throwing listener cannot take the connection
down — it is contained and reported on `diagnostics().events()` at `WARNING`.
Watch that stream during development: your rendering bugs appear there. The
one intentional exception: a throwing chat listener leaves the message
unacknowledged for redelivery ([5.7](#57-chat)).

---

## 10. Recipes

### 10.1 Download a whole folder from a browse

```java
Browse browse = slsk.users().browse(BrowseRequest.of(user));
Directory album = browse.at("Music\\Artist\\Album").orElseThrow();

List<DownloadRequest> requests = album.files().stream()
        .map(f -> DownloadRequest.builder(user, f.path(), Path.of("downloads", f.name()))
                .expectedSize(f.size())
                .tag("album", album.simpleName())
                .build())
        .toList();

List<TransferId> ids = slsk.downloads().enqueueAll(requests);
```

Enqueue everything at once. Queueing costs one line in the peer's queue and
opens no connections — the policy's concurrency caps apply only when transfers
start, and `maxConcurrentPerUser` (default 1) keeps the peer happy.

### 10.2 Survive a restart

```java
TransferStore store = new SqliteTransferStore(dbPath); // yours
Soulseek slsk = Soulseek.builder()
        .credentials(user, pass)
        .applicationMinorVersion(minor)
        .transferStore(store)
        .build();
```

The library saves every state change and restores the queue from `loadAll()`
at startup. Pair it with `TransferSink.file(...)` destinations: the `.part`
files left by interrupted downloads are what resumes pick up.

### 10.3 Be a good citizen (serve shares)

```java
Soulseek slsk = Soulseek.builder()
        .credentials(user, pass)
        .applicationMinorVersion(minor)
        .share(Path.of("/music"))
        .uploads(UploadPolicy.standard(2, 1))
        .profile(UserProfile.of("Sharing 12k files. Be nice."))
        .build();
slsk.connection().connect();
slsk.shares().rescan(); // index + announce counts
```

Peers check your share counts before serving you. A client that shares nothing
gets refused often — that is the network's culture, and `UserStatistics.sharesNothing()`
exists because policies check it.

### 10.4 Favor a friend

```java
slsk.uploads().all().stream()
        .filter(u -> u.user().equals(friend) && !u.isFinished())
        .forEach(u -> slsk.uploads().prioritize(u.id(), Priority.HIGH));
```

There is no buddy list in the library, deliberately. Who your friends are is
your application's knowledge. Priorities are how it reaches the queue.

### 10.5 Wishlist search (standing search)

```java
SearchId id = slsk.search().start(
        SearchQuery.of("rare bootleg 1974").withScope(SearchScope.wishlist()));
slsk.search().events().subscribe(SearchEvent.ResponsesReceived.class, e -> {
    if (e.id().equals(id)) notifyUser(e.responses());
});
```

The server paces wishlist searches on its own interval
(`connection().server()` reports it).

### 10.6 Watch a user come online

```java
Watch watch = slsk.users().watch(friend);
slsk.users().events().subscribe(UserEvent.StatusChanged.class, e -> {
    if (e.user().equals(friend) && e.to().isOnline()) notify(friend + " is online");
});
// keep `watch` open; watch.close() when no longer interested
```

The watch survives reconnects without your involvement, and watches on the
same user are reference-counted.

### 10.7 Export metrics

```java
scheduler.scheduleAtFixedRate(() -> {
    Metrics m = slsk.diagnostics().metrics();
    gauges.set("slsk_active_downloads", m.activeDownloads());
    gauges.set("slsk_bytes_downloaded", m.bytesDownloaded());
}, 0, 15, TimeUnit.SECONDS);
```

`metrics()` is a cheap snapshot built for exactly this polling.

---

## 11. Appendix: type index

Every exported type, one line each. Entry contracts live in `dev.slsk`;
capability models live in the package listed in [section 1](#1-the-library-at-a-glance).

**Entry contracts and capability models**

| Type | Kind | One line |
|---|---|---|
| `Soulseek` | interface | The client: ten facets and `close()`. |
| `SoulseekBuilder` | final class | Builds a `Soulseek`. Credentials and minor version required. |
| `Connection` | facet | Connect, disconnect, state, ping. |
| `Search` | facet | Start, run, await, stop searches. |
| `Downloads` | facet | The download queue and its commands. |
| `Uploads` | facet | Uploads peers requested; bans; upload policy. |
| `Users` | facet | Read about other users; browse; watch. |
| `Rooms` | facet | Join, leave, say; room state. |
| `PrivateRooms` | facet | Administer private rooms we own or moderate. |
| `Chat` | facet | Send and receive private messages. |
| `Shares` | facet | Configure, rescan, and replace what we share. |
| `Me` | facet | This account: presence, profile, privileges, password. |
| `Diagnostics` | facet | Log stream, metrics, mesh state, protocol trace. |
| `EventStream<T>` | interface | Subscribe to a facet's events, whole stream or one type. |
| `Subscription` | interface | Unregisters a listener. Idempotent close. |
| `Attachment<S>` | record | A snapshot and a subscription, taken atomically. |
| `Username` | record | A username, case-sensitive, exactly as it goes on the wire. |
| `TransferId` | record | Identifies one enqueue. Opaque. |
| `SearchId` | record | Identifies one search. Opaque. |
| `RemotePath` | static utility | Translate and validate backslash-joined wire paths. |
| `ServerAddress` | record | Host and port; `soulseek()` is the public server. |
| `ServerInfo` | record | What the server said about itself; all fields optional. |
| `ConnectionState` | sealed | Offline / Connecting / Authenticating / Online / Disconnecting / Reconnecting / Rejected. |
| `SearchQuery` | record | Terms, scope, limits, filters. |
| `SearchScope` | record | network / wishlist / room / users. |
| `SearchLimits` | record | Overall and idle timeouts, response caps. |
| `SearchFilters` | record | Bitrate, size, locked, extension filters, applied on arrival. |
| `SearchResult` | record | A finished search: every response kept. |
| `SearchSnapshot` | record | A search as it stands; `revision` for cheap change detection. |
| `SearchStatus` | enum | IN_PROGRESS / COMPLETED / CANCELLED / TIMED_OUT. |
| `SearchResponse` | record | One peer's answer: slots, speed, queue, files. |
| `SearchFile` | record | A file on offer: path, size, attributes. |
| `FileAttributes` | record | Typed accessors over the raw attribute map. |
| `FileAttributeType` | enum | Wire codes: bitrate, length, VBR, sample rate, bit depth. |
| `DownloadRequest` | record | What to fetch: user, path, sink, size, priority, tags. |
| `Download` | record | A download snapshot. |
| `Upload` | record | An upload snapshot. |
| `TransferState` | sealed | Queued / Requesting / QueuedRemotely / Connecting / Transferring / Paused / Finished. |
| `TransferOutcome` | sealed | Succeeded / Cancelled / Rejected / Failed. |
| `Progress` | record | Transferred, total, smoothed rate, honest eta. |
| `Priority` | enum | LOW / NORMAL / HIGH, for our own queue only. |
| `RejectionReason` | enum | Classified peer refusals; raw text always kept. |
| `DownloadPolicy` | record | Concurrency, per-peer cap, speed limit, poll interval, retry. |
| `RetryPolicy` | record | Attempts, backoff, which rejections are worth retrying. |
| `Bandwidth` | record | A rate limit with a named unit; 0 = unlimited. |
| `BrowseRequest` | record | Whose share to read, timeout, progress callback. |
| `Browse` | record | One user's whole share, flat, with lazy `files()`. |
| `BrowseProgress` | record | Bytes received of a browse in flight. |
| `BrowseResponse` | record | What we hand a peer who browses us (SPI-facing). |
| `Directory` | record | A shared directory: full remote path plus files. |
| `Room` | record | A joined room: users, tickers, ownership. No messages. |
| `RoomInfo` | record | A directory entry: name and user count. |
| `RoomList` | record | The server's room directory, split by our relationship. |
| `RoomTicker` | record | A pinned message, one per user. |
| `RoomUser` | record | A member with status, statistics, slots, country. |
| `UserInfo` | record | A peer's self-description. Untrusted display data. |
| `UserProfile` | record | What we tell peers about this account. |
| `UserStatistics` | record | Speed, upload count, share counts. |
| `UserStatus` | record | Presence plus privileged flag. |
| `UserPresence` | enum | OFFLINE / AWAY / ONLINE. |
| `Watch` | interface | An open, reference-counted, reconnect-surviving status subscription. |
| `SharedFolder` | record | A local folder offered to the network, optionally locked. |
| `ShareIndex` | record | What we share: counts, last scan, scan status. |
| `MeshState` | record | Our position in the distributed search mesh. |
| `Metrics` | record | Counter snapshot for exporters. |
| `DiagnosticLevel` | enum | NONE → TRACE, ordered. |

**`dev.slsk.events` — what you receive**

| Type | Members |
|---|---|
| `SoulseekEvent` | Root: every event has `at()`. |
| `ConnectionEvent` | StateChanged, ServerInfoReceived, KickedFromServer, GlobalMessageReceived, ExcludedSearchPhrasesReceived |
| `SearchEvent` | ResponsesReceived, StatusChanged, RequestReceived, ResponseDelivered, ResponseDeliveryFailed |
| `DownloadEvent` | Enqueued, StateChanged, Progressed, QueuePositionChanged, Finished, RetryScheduled, Forgotten |
| `UploadEvent` | Requested, StateChanged, Progressed, Finished, Denied |
| `ChatEvent` | MessageReceived |
| `RoomEvent` | Joined, Left, MessageReceived, UserJoined, UserLeft, TickerAdded, TickerRemoved, TickerListReceived, ListReceived, PublicChatMessageReceived, MembershipAdded, MembershipRemoved, ModerationAdded, ModerationRemoved |
| `UserEvent` | StatusChanged, StatisticsChanged, CannotConnect |
| `MeEvent` | LoggedIn, PrivilegeNotificationReceived, PrivilegedUserListReceived, PresenceChanged |
| `ShareEvent` | ScanStarted, ScanProgressed, ScanCompleted, BrowseServed |
| `MeshEvent` | StateChanged |
| `DiagnosticEvent` | (record, not sealed) level, source, message, exception, at |

**`dev.slsk.spi` — what you implement**

| Type | One line |
|---|---|
| `TransferSink` | Where download bytes go: open / commit / discard. Default: atomic file. |
| `TransferStore` | Queue persistence: save / delete / loadAll. Default: in memory. |
| `ShareCatalog` | The share, per requester: browse / directory / search / resolve / index. |
| `ResolvedFile` | A shared file the library can send: size + open(offset). |
| `UploadPolicy` | One decision per peer request: Allow / Queue(position) / Deny(reason). |
| `UploadRequest` | Who is asking for what, at what size. |
| `UploadContext` | Slots, queue depth, requester statistics and privilege. |

**`dev.slsk.exceptions`** — see [section 8](#8-exceptions).
