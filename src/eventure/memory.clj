(ns eventure.memory
  "A memory implementation of the core protocols."
  (:require [eventure.core :refer :all]
            [clojure.core.async :as a])
  (:import [java.net URI]))


;;; Helper functions.

(defmacro debug
  [& msgs]
  `(println "EVENTURE:" (apply str (interpose " " ~(vec msgs)))))


(defn- conj-set
  "Ensures a conj results in a set."
  [coll val]
  (set (conj coll val)))


;;; Directory service implementation in memory.

(defn- signal
  "Sends an update to the source watches."
  [event topic ^URI uri watches]
  (doseq [c (get @watches topic)]
    (debug "Signal" event "about" topic "for" uri "to" c)
    (when-not (a/put! c (->SourceUpdate topic uri event))
      (debug "Watch" c "on" topic "closed, removing from directory service.")
      (swap! watches update-in [topic] disj c))))


;; sources = {topic (uri uri uri)} where first is latest join
;; watches = {topic #{chan chan chan}}
(defrecord MemoryDirectory [sources watches]
  DirectoryWriter
  (add-source [this topic uri]
    (when (dosync
            (when (empty? (filter #(= % uri) (get @sources topic)))
              (alter sources update-in [topic] conj uri)))
      (debug "Set source" uri "for" topic)
      (signal :joined topic uri watches)))

  (remove-source [this topic uri]
    (when (dosync
            (let [current (get @sources topic)
                  removed (remove #(= % uri) current)]
              (when-not (= current removed)
                (alter sources assoc topic removed))))
      (debug "Unset source" uri "for" topic)
      (signal :left topic uri watches)))

  DirectoryReader
  (sources [this topic]
    (get @sources topic))

  (watch-sources [this topic init]
    (watch-sources this topic init (a/chan)))

  (watch-sources [this topic init chan]
    (debug "Add watch" chan "on" topic "using init" init)
    (swap! watches update-in [topic] conj-set chan)
    (when-let [sources (seq (get @sources topic))]
      (case init
        :last (a/put! chan (->SourceUpdate topic (first sources) :joined))
        :all (doseq [source sources] (a/put! chan (->SourceUpdate topic source :joined)))
        :random (a/put! chan (->SourceUpdate topic (rand-nth sources) :joined))))
    chan)

  (unwatch-sources [this topic chan]
    (debug "Removing watch" chan "on" topic)
    (swap! watches update-in [topic] disj chan)
    chan))


(defn mk-directory
  []
  (MemoryDirectory. (ref {}) (atom {})))


;;; Server implementation using core.async.

(def ^:private ca-servers (atom {})) ; uri -> CoreAsyncServer
(def ^:private ca-mults (atom {})) ; [uri topic] -> mult

(defn- channel-for-topic
  [{:keys [uri directory channels] :as server} topic]
  (debug "Server" uri "requested channel for topic" topic)
  (if-let [channel (get @channels topic)]
    channel
    (let [channel (a/chan)]
      (debug "Creating and registering new channel for topic" topic "for server" uri)
      (swap! ca-mults assoc [uri topic] (a/mult channel))
      (swap! channels assoc topic channel)
      (add-source directory topic uri)
      channel)))

;; channels = {topic chan}
;; cache = ?
(defrecord CoreAsyncServer [uri directory channels cache cache-size open]
  Server
  (publish [this topic event]
    (debug "Publishing" event "for" topic "through server for" uri)
    (assert @open)
    (let [channel (channel-for-topic this topic)]
      (a/put! channel event)
      (swap! cache update-in [topic] (fn [c] (take cache-size (conj c event))))))

  (done [this topic]
    (debug "Events for" topic "through server for" uri "are done")
    (assert @open)
    (when-let [channel (get @channels topic)]
      (swap! channels dissoc topic)
      (swap! ca-mults dissoc [uri topic])
      (a/close! channel)
      (remove-source directory topic uri))))


(defn mk-server
  [name cache-size directory]
  (debug "Creating core.async server named" name)
  (let [uri (URI. (str "ca://" name))
        server (CoreAsyncServer. uri directory (atom {}) (atom {}) cache-size (atom true))]
    (assert (not (get @ca-servers uri)))
    (swap! ca-servers assoc uri server)
    server))


(defn stop-server
  [{:keys [uri open channels directory] :as server}]
  (when @open
    (debug "Stopping server for" uri)
    (reset! open false)
    (doseq [[topic channel] @channels]
      (swap! ca-mults dissoc [name topic])
      (a/close! channel)
      (remove-source directory topic uri))
    (swap! ca-servers dissoc uri)))


;;; Client functions using core.async server.

(defn subscribe
  "Subscribes a, possibly default unbuffered, channel to the given
  topic for the given server URI. The channel will close, whenever the
  server does not publish on the specified topic or whenever it is
  done sending messages on that topic. The `last` parameter is the
  number of last messages one wants to receive first on the channel."
  ([^URI uri topic last]
     (subscribe uri topic last (a/chan)))
  ([^URI uri topic last channel]
     (debug "Subscribing to topic" topic "on server" uri "using channel" channel)
     (if-let [{:keys [cache cache-size] :as server} (get @ca-servers uri)]
       ;; Even when publisher is done, an open server may still send the last cached messages.
       (let [cmessages (take last (reverse (get @cache topic)))]
         (debug "Sending" (count cmessages) "cached messages for topic" topic "to" channel)
         (doseq [event cmessages] (a/put! channel event))
         (if-let [topicm (get @ca-mults [uri topic])]
           (do (debug "Tapping" channel "to topic" topic)
               (a/tap topicm channel))
           (a/close! channel)))
       (a/close! channel))
     channel))


(defn unsubscribe
  "Unsubscribes the channel from the topic fro the given server uri.
  Won't close the channel."
  [^URI uri topic channel]
  (when-let [topicm (get @ca-mults [uri topic])]
    (debug "Unsubscribing channel" channel "from topic" topic "on server" uri)
    (a/untap topicm channel)))
