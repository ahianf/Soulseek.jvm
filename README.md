# Soulseek.jvm

Soulseek.jvm is a Java library for the Soulseek peer-to-peer network. It offers
a blocking, dependency-free API for connecting, searching, browsing shares,
transferring files, chatting, and observing network state.

The [complete public API reference](docs/public-api.md) documents every facet,
event, value, exception, and extension point. The supported namespace is
`dev.slsk`; the old `tenine` classes are not part of the public API.

## Requirements and installation

The current build requires JDK 25 or newer and Maven 3.9 or newer. Install the
artifact into your local Maven repository:

```shell
mvn install
```

Then add it to an application:

```xml
<dependency>
  <groupId>dev.slsk</groupId>
  <artifactId>slsk-jvm</artifactId>
  <version>2.1.0</version>
</dependency>
```

## Connect

Create one process-lifetime client. Building does not connect; the connection
facet performs login explicitly.

```java
import dev.slsk.Soulseek;

try (Soulseek slsk = Soulseek.builder()
        .credentials(System.getenv("SOULSEEK_USERNAME"),
                     System.getenv("SOULSEEK_PASSWORD"))
        .applicationMinorVersion(1234)
        .build()) {
    slsk.connection().connect();
    System.out.println(slsk.connection().state());
}
```

Soulseek has no anonymous login. Choose an application minor version greater
than 100 and keep it stable for a given application build.

## Search and download

Searches block until they complete or the calling thread is interrupted.
Results are immutable snapshots that can be handed directly to a download
request.

```java
import java.nio.file.Path;
import dev.slsk.download.Download;
import dev.slsk.download.DownloadRequest;
import dev.slsk.search.SearchFile;
import dev.slsk.search.SearchQuery;
import dev.slsk.search.SearchResponse;
import dev.slsk.search.SearchResult;
import dev.slsk.transfer.TransferId;

SearchResult result = slsk.search().run(SearchQuery.of("artist album"));

if (!result.isEmpty()) {
    SearchResponse source = result.responses().getFirst();
    SearchFile file = source.files().getFirst();
    TransferId id = slsk.downloads().enqueue(DownloadRequest.of(
            source.user(), file, Path.of("downloads", file.name())));
    Download finished = slsk.downloads().await(id);
    System.out.println(finished.state());
}
```

See the [quickstart](docs/public-api.md#2-quickstart-download-a-file) for source
selection and exhaustive transfer-outcome handling.

## Philosophy: blocking code on virtual threads

Java 21 made virtual threads the natural concurrency model for network
libraries. Soulseek.jvm follows that model directly: operations block, callers
compose them with ordinary control flow, and concurrent work gets one virtual
thread instead of a `CompletableFuture` graph.

```java
Thread.startVirtualThread(() ->
        slsk.search().run(SearchQuery.of("live recording")));
```

Cancellation is thread interruption, the platform's own contract: every
blocking call declares `throws InterruptedException`, so `Future.cancel(true)`
and `ExecutorService.shutdownNow()` cancel library calls the same way they
cancel a `BlockingQueue.take()`. Each blocking call also has a `Duration`
overload that throws `TimeoutException` when the caller's deadline expires.
The library keeps all socket I/O on threads it owns — your thread only ever
parks on `java.util.concurrent` primitives, which is what makes interrupting
it safe. State is exposed as immutable snapshots, while sealed event streams
carry deltas. The runtime artifact has no third-party dependencies.

Although the programming model starts with Java 21, the current runtime
baseline is Java 25. The implementation uses monitors on network hot paths, and
Java 25's JEP 491 prevents those monitors from pinning virtual-thread carrier
threads.

## License and lineage

Soulseek.jvm is based on Soulseek.NET 10.0.2 and is not maintained by, endorsed
by, or affiliated with that project. See [LICENSE](LICENSE) and
[NOTICE](NOTICE) for the applicable terms and attribution.
