# pubsure-core

> Pub? Sure!

Pubsure is a set of Clojure protocols that together form a publish-subscribe framework. Some of its properties are:

* A central directory service is used for registering to publisher sources for a specific topic. Potential subscribers use this directory to find the publishers.

* The protocols do not deal with semantics like: 
  * whether all subscribed clients receive all messages, or some load-balancing or work distribution takes place;
  * Starting, stopping and communication between the components;
  * How to deal with issues like the _slow joiner_ problem;

* The directory service API uses core.async channels to inform clients about joined and left publishers.

## API

### The DirectoryWriter protocol

This protocol is for adding and removing URIs as a sources of messages on the given topic.

* `add-source [this topic uri]` - Given a topic, this function writes the given URI to the directory service.

* `remove-source [this topic uri]` - Given a topic, this function removes the given URI from the directory service.

### The DirectoryReader protocol

This protocol is for retrieving a one-time batch of current topic sources, or for continuesly receiving updates on joined and left sources. Clojure clients can talk to a DirectoryReader instance directly. In all other cases, the Clojure protocol needs to be exposed in some other way.

* `sources [this topic]` - This function returns the currently known sources (URIs) of the given topic.

* `watch-sources [this topic init] [this topic init chan]` - This function starts a backgrould loop which reads the directory service and puts updates about sources for the given topic on a, possibly supplied, channel. The channel is returned. Whenever a channel is closed, all watches for that channel are removed. Updates on the channel are SourceUpdate records. The init parameter is one of the following:

  * `:all` - all currently known sources are put on the channel.

  * `:last` - the last joined source is put on the channel.

  * `:random` - a randomly chosen source is put on the channel.")

* `unwatch-sources [this identifier chan]` - The given channel won't receive any updates on the directory
     service anymore regarding the given topic. Returns the channel.

### The Source protocol

This protocol is for publishing messages. Starting, stopping and accepting connections to a Source is not part of this protocol.

* `publish [this topic message]` - Publish a message on the given topic. The Source needs to register the topic automatically in the directory service. I.e. the Source implementation needs a DirectoryWriter instance.

* `done [this topic]` - Notify the Publisher that the no more messages for the given topic will be published (for now). The Publisher will be unregistered from the directory service."

## Current implementations

### Zookeeper

The [pubsure-zk](#) project holds a `DirectoryWriter` and `DirectoryReader` implementation using a Zookeeper cluster as its store.

### Websockets

The [pubsure-ws](#) project holds a `Source` implementation, using Websockets publishing messages to its subscribers.

### Memory

This project also contains a memory implementation of all three protocols, and two simple `subscribe` / `unsubscribe` functions for getting messages over core.async channels. This memory implementation can be used for testing other implementations or for mocking in other systems.

## TODO

* Decide whether some more semantics should be formalised, in favor of interoperabillity between various implementations. E.g., support X last messages on subscribe by default, or have all published messages be send to all subscribers at that time.


## License

Copyright © 2014

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
