(ns pubsure.memory
  "A memory implementation of the core protocols."
  (:require [pubsure.core :refer :all]
            [pubsure.utils :refer (with-debug conj-set)]
            [clojure.core.async :as a])
  (:import [java.net URI]))


;;; Directory service implementation in memory.

(defn- signal
  "Sends an update to the source watches."
  [event topic ^URI uri watches]
  (doseq [c (get @watches topic)]
    (with-debug pubsure [(println "Signal" event "about" topic "for" uri "to" c)])
    (when-not (a/put! c (->SourceUpdate topic uri event))
      (with-debug pubsure
        [(println "Watch" c "on" topic "closed, removing from directory service.")])
      (swap! watches update-in [topic] disj c))))


;; sources = {topic (uri uri uri)} where first is latest join
;; watches = {topic #{chan chan chan}}
(defrecord MemoryDirectory [sources watches]
  DirectoryWriter
  (add-source [this topic uri]
    (when (dosync
            (when (empty? (filter #(= % uri) (get @sources topic)))
              (alter sources update-in [topic] conj uri)))
      (with-debug pubsure [(println "Set source" uri "for" topic)])
      (signal :joined topic uri watches)))

  (remove-source [this topic uri]
    (when (dosync
            (let [current (get @sources topic)
                  removed (remove #(= % uri) current)]
              (when-not (= current removed)
                (alter sources assoc topic removed))))
      (with-debug pubsure [(println "Unset source" uri "for" topic)])
      (signal :left topic uri watches)))

  DirectoryReader
  (sources [this topic]
    (get @sources topic))

  (watch-sources [this topic init]
    (watch-sources this topic init (a/chan)))

  (watch-sources [this topic init chan]
    (with-debug pubsure [(println "Add watch" chan "on" topic "using init" init)])
    (swap! watches update-in [topic] conj-set chan)
    (when-let [sources (seq (get @sources topic))]
      (case init
        :last (a/put! chan (->SourceUpdate topic (first sources) :joined))
        :all (doseq [source sources] (a/put! chan (->SourceUpdate topic source :joined)))
        :random (a/put! chan (->SourceUpdate topic (rand-nth sources) :joined))))
    chan)

  (unwatch-sources [this topic chan]
    (with-debug pubsure [(println "Removing watch" chan "on" topic)])
    (swap! watches update-in [topic] disj chan)
    chan))


(defn mk-directory
  []
  (MemoryDirectory. (ref {}) (atom {})))


;;; Source implementation using core.async.

(def ^:private ca-sources (atom {})) ; uri -> CoreAsyncServer
(def ^:private ca-mults (atom {})) ; [uri topic] -> mult

(defn- channel-for-topic
  [{:keys [uri directory channels] :as source} topic]
  (with-debug pubsure [(println "Server" uri "requested channel for topic" topic)])
  (if-let [channel (get @channels topic)]
    channel
    (let [channel (a/chan)]
      (with-debug pubsure [(println "Creating and registering new channel for topic" topic
                                     "for source" uri)])
      (swap! ca-mults assoc [uri topic] (a/mult channel))
      (swap! channels assoc topic channel)
      (add-source directory topic uri)
      channel)))

;; channels = {topic chan}
;; cache = ?
(defrecord CoreAsyncSource [uri directory channels cache cache-size open]
  Source
  (publish [this topic message]
    (with-debug pubsure [(println "Publishing" message "for" topic "through source for" uri)])
    (assert @open)
    (let [channel (channel-for-topic this topic)]
      (a/put! channel message)
      (swap! cache update-in [topic] (fn [c] (take cache-size (conj c message))))))

  (done [this topic]
    (with-debug pubsure [(println "Events for" topic "through source for" uri "are done")])
    (assert @open)
    (when-let [channel (get @channels topic)]
      (swap! channels dissoc topic)
      (swap! ca-mults dissoc [uri topic])
      (a/close! channel)
      (remove-source directory topic uri))))


(defn mk-source
  [name cache-size directory]
  (with-debug pubsure [(println "Creating core.async source named" name)])
  (let [uri (URI. (str "ca://" name))
        source (CoreAsyncSource. uri directory (atom {}) (atom {}) cache-size (atom true))]
    (assert (not (get @ca-sources uri)))
    (swap! ca-sources assoc uri source)
    source))


(defn stop-source
  [{:keys [uri open channels directory] :as source}]
  (when @open
    (with-debug pubsure [(println "Stopping source for" uri)])
    (reset! open false)
    (doseq [[topic channel] @channels]
      (swap! ca-mults dissoc [name topic])
      (a/close! channel)
      (remove-source directory topic uri))
    (swap! ca-sources dissoc uri)))


;;; Client functions using core.async source.

(defn subscribe
  "Subscribes a, possibly default unbuffered, channel to the given
  topic for the given source URI. The channel will close, whenever the
  source does not publish on the specified topic or whenever it is
  done sending messages on that topic. The `last` parameter is the
  number of last messages one wants to receive first on the channel."
  ([^URI uri topic last]
     (subscribe uri topic last (a/chan)))
  ([^URI uri topic last channel]
     (with-debug pubsure [(println "Subscribing to topic" topic "on source" uri
                                    "using channel" channel)])
     (if-let [{:keys [cache cache-size] :as source} (get @ca-sources uri)]
       ;; Even when publisher is done, an open source may still send the last cached messages.
       (let [cmessages (take last (reverse (get @cache topic)))]
         (with-debug pubsure [(println "Sending" (count cmessages) "cached messages for topic"
                                        topic "to" channel)])
         (doseq [message cmessages] (a/put! channel message))
         (if-let [topicm (get @ca-mults [uri topic])]
           (do (with-debug pubsure [(println "Tapping" channel "to topic" topic)])
               (a/tap topicm channel))
           (a/close! channel)))
       (a/close! channel))
     channel))


(defn unsubscribe
  "Unsubscribes the channel from the topic fro the given source uri.
  Won't close the channel."
  [^URI uri topic channel]
  (when-let [topicm (get @ca-mults [uri topic])]
    (with-debug pubsure [(println "Unsubscribing channel" channel "from topic" topic
                                   "on source" uri)])
    (a/untap topicm channel)))
