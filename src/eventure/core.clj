(ns eventure.core
  (:require [eventure.protocols :as p]
            [clojure.core.async :as a :refer (<!! <!)])
  (:import [java.net URI]))


(def ^:private reusable-clients (atom {})) ; uri -> client
(def ^:private subscriptions (atom {})) ; identifier -> commandc


(defn- client-for-uri
  "Creates or reuses an existing client for the given URI."
  [^URI uri]
  (locking reusable-clients
    (or (get @reusable-clients uri)
        (let [client (p/mk-client uri)]
          (when (p/reusable? client)
            (swap! reusable-clients assoc uri client))
          client))))


(defn- subscribe*
  "Starts the loop of watching the source channel, and piping
  subscribed clients to the given events channel. The command channel
  is used for stopping the subscription."
  [identifier channel sourcesc commandc {:keys [on-source-join wait-for-source]
                                         :or {wait-for-source -1}
                                         :as options}]
  (let [eventm (a/mix channel)]
    (a/go-loop [waitc (when-not (neg? wait-for-source)
                        (a/timeout wait-for-source))
                clients {}]  ; uri -> client
      (let [[val port] (a/alts! (keep identity [commandc sourcesc waitc]) :priority true)]
        (condp = port
          sourcesc (let [{:keys [event uri]} val]
                     (case event
                       :joined (case on-source-join
                                 :mix (let [client (client-for-uri uri)]
                                        (a/admix eventm (p/subscribe client identifier))
                                        (recur nil (assoc clients uri client)))
                                 :first (if (empty? clients)
                                          (let [client (client-for-uri uri)]
                                            (a/admix eventm (p/subscribe client identifier))
                                            (recur nil {uri client}))
                                          (recur nil clients))
                                 :last (do (when-let [client (first (vals clients))]
                                             (p/unsubscribe client identifier))
                                           (let [client (client-for-uri uri)]
                                             (a/admix eventm (p/subscribe client identifier))
                                             (recur nil {uri client})))
                                 :unsubscribe (if (empty? clients)
                                                (let [client (client-for-uri uri)]
                                                  (a/admix eventm (p/subscribe client identifier))
                                                  (recur nil {uri client}))
                                                (do (a/put! commandc :unsubscribe)
                                                    (recur nil clients))))
                       :left (let [client (get clients uri)
                                   new-clients (dissoc clients uri)
                                   new-waitc (when (and (empty? new-clients) (> wait-for-source 0))
                                               (a/timeout wait-for-source))]
                               (when client (p/unsubscribe client identifier))
                               (when (and (empty? new-clients) (= wait-for-source 0))
                                 (a/put! commandc :unsubscribe))
                               (recur new-waitc new-clients))))
          commandc (if (= val :unsubscribe)
                     (do (doseq [[_ client] clients]
                           (p/unsubscribe client identifier))
                         (doseq [c (keep identity [channel sourcesc commandc waitc])]
                           (a/close! c))
                         (swap! subscriptions dissoc identifier))
                     (recur waitc clients))
          waitc (do (a/put! commandc :unsubscribe)
                    (recur nil clients)))))))


(defn subscribe
  "Subscribe to an event stream named by the given identifier. The
  events will be put on the, possibly supplied, channel. Returns the
  channel. Some other options can be supplied as well:

  on-source-join

    Whenever a new source joins and the subscription is already
    connected to a source, the join is handled depending on the value
    of this option:

      :mix - the subscription will also start receiving events from
             the new source. (default)

      :first - the newly joined source is ignored, and the
               subscription will continue to receive events from the
               first source it was connected to.

      :last - the subscription will start to receive events from the
              newly joined source, and will stop receiving events from
              the current source.

      :unsubscribe - the subscription will be unsubscribed
                     automatically.

  wait-for-source

    The number of milliseconds to wait for a new source to join when
    no sources were found or when all sources have left. A negative
    value indicates that it will wait indefinitely. Default is -1.

  channel

    The channel to use for outputting the events."
  [registry identifier & {:keys [on-source-join channel]
                          :or {on-source-join :mix
                               channel (a/chan)}
                          :as options}]
  (locking (.intern identifier)
    (if (get @subscriptions identifier)
      (throw (IllegalArgumentException. "cannot subscribe to an identifier twice"))
      (let [sourcesc (p/watch-source registry identifier (if (= :mix on-source-join) :all :last))
            commandc (a/chan)]
        (subscribe* identifier channel sourcesc commandc
                    (assoc options :on-source-join on-source-join))
        (swap! subscriptions assoc identifier commandc)
        channel))))


(defn unsubscribe
  "Unsubscribes the given identifier and closes the event channel.
  Returns a future, holding true whenver the unsubscription is
  complete, or false whenever there was nothing to unsubscribe."
  [identifier]
  (if-let [commandc (get @subscriptions identifier)]
    (do (println "Unsubscribing" identifier)
        (a/put! commandc :unsubscribe)
        (future (loop []
                  (if (get @subscriptions identifier)
                    (do (println "Still unsubscribing" identifier)
                        (Thread/sleep 100)
                        (recur))
                    true))))
    (future false)))


(def publish p/publish)
(def done p/done)
