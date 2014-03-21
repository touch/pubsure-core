(ns eventure.protocols
  (:import [java.net URI]))

;;; Protocol definitions.

(defrecord SourceUpdate [identifier uri event]) ; Field `event` is either :joined or :left

(defprotocol Registry
  (set-source [this identifier uri]
    "Given an identifier, this function writes the given URI to the
    registry. This function is used by a Server implementation.")

  (unset-source [this identifier uri]
    "Given an identifier, this function removes the given URI from the
    registry. This function is used by a Server impementation.")

  (watch-source [this identifier init] [this identifier init chan]
    "Given an identifier, this function reads the registry and puts
    updates to it on a, possibly supplied, channel. The channel is
    returned. Whenever a channel is closed, all watches for that
    channel are removed. Updates on the channel are SourceUpdate
    records. The init parameter is either :last or :all. When it is
    :last, the last joined and still active source is put on the
    channel immediatly. When it is :all, all known sources are put on
    the channel directly. This function is used by the `core`
    namespace.")

  (unwatch-source [this identifier chan]
    "The given channel won't receive any updates on the registry anymore
    regarding the given identifier. Returns the channel. This function
    is used by the `core` namespace."))


(defprotocol Client
  (reusable? [this]
    "Returns truthy if the same connection can be used for multiple
    subscriptions and unsubscriptions. Preferably true.")

  (subscribe [this identifier] [this identifier chan]
    "Subscribes the, possibly supplied, channel to events for the given
    identifier. Returns the channel, or nil if the source does not
    have events for identifier. Closing the channel unsubscribes
    automatically.")

  (unsubscribe [this identifier]
    "Unsubscribes for the events for the given identifier. The
    subscribed channel will be closed.")

  (disconnect [this]
    "Unsubscribes all event channels for this channel and closes the
    connection the event server. Only called when the Client is
    reusable."))


(defprotocol Server
  (publish [this identifier event]
    "Publish an event on the stream named by the identifier. The event
    source will be registered automatically. I.e. the server
    implementation needs an Registry instance.")

  (done [this identifier]
    "Notify the server that the event source named by the identifier is
    done. The event source will be unregistered.")

  (stop [this]
    "Stop accepting subscriptions and close the open subscriptions."))


;;; Multi-method definitions.

(defmulti mk-client
  "Creates a Client instance based on the scheme of the given URI.
  This URI is probably retrieved from a Registry."
  (fn [^URI uri] (.getScheme uri)))


(defmethod mk-client :default
  [^URI uri]
  (throw (IllegalArgumentException.
          (str "Could not create Client for protocol " (.getScheme uri) ". "
               "Did you load the namespace having the mk-client method for this protocol?"))))
