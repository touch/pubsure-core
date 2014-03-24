(ns eventure.core
  "The eventure API."
  (:import [java.net URI]))

;;; Protocol definitions.

(defprotocol DirectoryWriter
  (add-source [this topic uri]
    "Given a topic, this function writes the given URI to the directory
    service.")

  (remove-source [this topic uri]
    "Given a topic, this function removes the given URI from the
    directory service."))


;; Record used for notifying watchers about changes in the directory.
;; Field `event` is either :joined or :left.
(defrecord SourceUpdate [topic uri event])


(defprotocol DirectoryReader
  (sources [this topic]
    "This function returns the currently known sources of the given
    topic.")

  (watch-sources [this topic init] [this topic init chan]
    "This function starts a backgrould loop which reads the directory
    service and puts updates about sources for the given topic on a,
    possibly supplied, channel. The channel is returned. Whenever a
    channel is closed, all watches for that channel are removed.
    Updates on the channel are SourceUpdate records. The init
    parameter is one of the following:

    :all - all currently known sources are put on the channel.

    :last - the last joined source is put on the channel.

    :random - a randomly chosen source is put on the channel.")

  (unwatch-sources [this identifier chan]
    "The given channel won't receive any updates on the directory
     service anymore regarding the given topic. Returns the channel."))


(defprotocol Server
  (publish [this identifier event]
    "Publish an event on the given topic. The event source will be
    registered automatically. I.e. the server implementation needs a
    DirectoryWriter instance.")

  (done [this identifier]
    "Notify the server that the no more events for the given topic will
    be published (for now). The event source will be unregistered from
    the directory service."))
