(ns pubsure.core
  "The pubsure API."
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

    :random - a randomly chosen source is put on the channel.

    :none - no current source is put on the channel.")

  (unwatch-sources [this identifier chan]
    "The given channel won't receive any updates on the directory
     service anymore regarding the given topic. Returns the channel."))


(defprotocol Source
  (publish [this topic message]
    "Publish a message on the given topic. The Source will be
    registered automatically in the directory service. I.e. the server
    implementation needs a DirectoryWriter instance.")

  (done [this topic]
    "Notify the Source that the no more messages for the given topic
    will be published (for now). The Source will be unregistered
    from the directory service, regarding the given topic."))
