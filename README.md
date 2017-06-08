NetflixGraph
============

[![NetflixOSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/netflix-graph.svg)](#)

NetflixGraph is a compact in-memory data structure used to represent directed graph data. You can use NetflixGraph to vastly reduce the size of your application’s memory footprint, potentially by an order of magnitude or more. If your application is I/O bound, you may be able to remove that bottleneck by holding your entire dataset in RAM. This may be possible with NetflixGraph; you’ll likely be very surprised by how little memory is actually required to represent your data.

NetflixGraph provides an API to translate your data into a graph format, compress that data in memory, then serialize the compressed in-memory representation of the data so that it may be easily transported across your infrastructure.

Artifacts
---------

The NetflixGraph binaries are published to Maven Central.

|GroupID/Org|ArtifactID/Name|Latest Version|
| --------- | ------------- |--------------|
|com.netflix.nfgraph|netflix-graph|1.5.2|

        ...	
        <dependency>
        	<groupId>com.netflix.nfgraph</groupId>
        	<artifactId>netflix-graph</artifactId>
                <version>1.5.2</version>
        </dependency>
        ...

Features
--------
A quick overview can be found either on the [wiki](https://github.com/Netflix/netflix-graph/wiki) or on the [Netflix Tech Blog](http://techblog.netflix.com/2013/01/netflixgraph-metadata-library_18.html).

Since the blog article was published, a number of improvements have been made:

* contains() operations on connection sets are no longer necessarily O(n).  Some memory-efficiency can be sacrificed to obtain O(1) performance.
* The library will automatically switch from the default encoding to bit set encoding if it is more memory efficient.
* Interfaces have been added to efficiently map Objects to ordinals.


Documentation
-------------
Detailed documentation of NetflixGraph's features and usage can be found on the [wiki](https://github.com/Netflix/netflix-graph/wiki).

Javadocs are available [here](http://netflix.github.com/netflix-graph/javadoc).


Build
-----

NetflixGraph is built via Gradle (www.gradle.org). To build from the command line:

    ./gradlew build


Support
-------
Support can be obtained through the [NetflixGraph google group](https://groups.google.com/group/netflix-graph)
