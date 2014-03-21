(ns eventure.memory
  (:require [eventure.protocols :refer :all]
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


;;; Registry implementation.

(defn- signal
  "Sends an update to the source watches."
  [event identifier ^URI uri watches]
  (doseq [c (get @watches identifier)]
    (debug "Signal" event "about" identifier "for" uri "to" c)
    (when-not (a/put! c (->SourceUpdate identifier uri event))
      (debug "Watch" c "on" identifier "closed, removing from registry.")
      (swap! watches update-in [identifier] disj c))))


;; sources = {identifier (uri uri uri)} where first is latest join
;; watches = {identifier #{chan chan chan}}
(defrecord MemoryRegistry [sources watches]
  Registry
  (set-source [this identifier uri]
    (let [updated (atom false)]
      (dosync
        (if (empty? (filter #(= % uri) (get @sources identifier)))
          (do (alter sources update-in [identifier] conj uri)
              (reset! updated true))
          (reset! updated false)))
      (when @updated
        (debug "Set source" uri "for" identifier)
        (signal :joined identifier uri watches))))

  (unset-source [this identifier uri]
    (let [updated (atom false)]
      (dosync
        (let [current (get @sources identifier)
              removed (remove #(= % uri) current)]
          (if-not (= current removed)
            (do (alter sources assoc identifier removed)
                (reset! updated true))
            (reset! updated false))))
      (when @updated
        (debug "Unset source" uri "for" identifier)
        (signal :left identifier uri watches))))

  (watch-source [this identifier init]
    (watch-source this identifier init (a/chan)))

  (watch-source [this identifier init chan]
    (debug "Add watch" chan "on" identifier "using init" init)
    (swap! watches update-in [identifier] conj-set chan)
    (when-let [sources (seq (get @sources identifier))]
      (case init
        :last (a/put! chan (->SourceUpdate identifier (first sources) :joined))
        :all (doseq [source sources]
               (a/put! chan (->SourceUpdate identifier source :joined)))))
    chan)

  (unwatch-source [this identifier chan]
    (debug "Removing watch" chan "on" identifier)
    (swap! watches update-in [identifier] disj chan)
    chan))


(defn ->MemoryRegistry
  []
  (MemoryRegistry. (ref {}) (atom {})))


;;; Client implementation.

(def ^:private streams (atom {})) ; {[uri identifier] mult}

;; subscriptions = {identifier chan}
(defrecord MemoryClient [^URI uri subscriptions]
  Client
  (reusable? [this]
    true)

  (subscribe [this identifier]
    (subscribe this identifier (a/chan)))

  (subscribe [this identifier chan]
    (debug "Client for" uri "subscribing" chan "to" identifier)
    (assert (not (get @subscriptions identifier)))
    (when-let [mult (get @streams [uri identifier])]
      (a/tap mult chan)
      (swap! subscriptions assoc identifier chan)
      chan))

  (unsubscribe [this identifier]
    (when-let [chan (get @subscriptions identifier)]
      (when-let [mult (get @streams [uri identifier])]
        (debug "Client for" uri "unsubscribing" chan "from" identifier)
        (a/untap mult chan)
        (swap! subscriptions dissoc identifier))))

  (disconnect [this]
    (doseq [chan (vals @subscriptions)]
      (a/close! chan))))


(defmethod mk-client "memory"
  [^URI uri]
  (debug "Creating Memory Client for" uri)
  (MemoryClient. uri (atom {})))


;;; Server implementation.

(def ^:private servers (atom #{}))

(defn- channel-for-identifier
  [registry channels uri identifier]
  (debug "Server for" uri "requested channel for" identifier)
  (if-let [channel (get @channels identifier)]
    channel
    (let [channel (a/chan)]
      (debug "Server for" uri "creating new channel for" identifier)
      (swap! streams assoc [uri identifier] (a/mult channel))
      (swap! channels assoc identifier channel)
      (set-source registry identifier uri)
      channel)))

;; channels = {identifier chan}
(defrecord MemoryServer [uri registry channels open send-last-event]
  Server
  (publish [this identifier event]
    (debug "Publishing" event "for" identifier "through server for" uri)
    (assert @open)
    (let [channel (channel-for-identifier registry channels uri identifier)]
      ;; Need inverted pub-sub (consumer is server), so watching subscriptions won't miss messages
      ;; while they are processing the :joined source event?
      ;;
      ;; topics/
      ;;   test-topic/
      ;;     pubs/
      ;;       1: tcp://source-1
      ;;       2: tcp://source-2
      ;;     subs/
      ;;       1: tcp://sub-1
      ;;       2: tcp://sub-2
      ;;       3: tcp://sub-3
      (Thread/sleep 1000)
      (a/put! channel event)))

  (done [this identifier]
    (debug "Events for" identifier "through server for" uri "are done")
    (assert @open)
    (when-let [channel (get @channels identifier)]
      (swap! channels dissoc identifier)
      (swap! streams dissoc [uri identifier])
      (a/close! channel)
      (unset-source registry identifier uri)))

  (stop [this]
    (when @open
      (debug "Stopping server for" uri)
      (reset! open false)
      (doseq [[identifier channel] @channels]
        (swap! channels dissoc identifier)
        (swap! streams dissoc [uri identifier])
        (a/close! channel)
        (unset-source registry identifier uri))
      (swap! servers disj uri))))


(defn mk-memory-server
  [host registry]
  (debug "Creating Memory Server for" host)
  (let [uri (URI. (str "memory://" host))]
    (assert (not (get @servers uri)))
    (swap! servers conj uri)
    (MemoryServer. uri registry (atom {}) (atom true) false)))
