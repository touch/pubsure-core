(ns eventure.core-test
  "Testing the model using the memory implementation."
  (:require [clojure.test :refer :all]
            [eventure.core :refer :all]
            [eventure.memory :as m]
            [clojure.core.async :as a])
  (:import [java.net URI]))


(deftest directory
  (let [directory (m/mk-directory)]

    ;; Register sources without watchers.
    (let [source-1 (URI. "ca://source-1")
          source-2 (URI. "ca://source-2")]
      (add-source directory "test-topic" source-1)
      (add-source directory "test-topic" source-2)
      (let [ss (sources directory "test-topic")]
        (is (every? #{source-1 source-2} ss))
        (is (= 2 (count ss)))))

    (let [sourcesc (watch-sources directory "test-topic" :last)
          [val _] (a/alts!! [sourcesc (a/timeout 1000)])]
      ;; Test it only got the last set source.
      (is (= val (->SourceUpdate "test-topic" (URI. "ca://source-2") :joined)))

      ;; Test is receives subsequent updates.
      (remove-source directory "test-topic" (URI. "ca://source-1"))
      (let [[val _] (a/alts!! [sourcesc (a/timeout 1000)])]
        (is (= val (->SourceUpdate "test-topic" (URI. "ca://source-1") :left)))))

    ;; Test a new watcher receiving all registered sources.
    (add-source directory "test-topic" (URI. "ca://source-1"))
    (let [sourcesc (watch-sources directory "test-topic" :all)
          [val1 _] (a/alts!! [sourcesc (a/timeout 1000)])
          [val2 _] (a/alts!! [sourcesc (a/timeout 1000)])
          vals (hash-set val1 val2)]
      (is (get vals (->SourceUpdate "test-topic" (URI. "ca://source-1") :joined)))
      (is (get vals (->SourceUpdate "test-topic" (URI. "ca://source-2") :joined))))))


(deftest pub-sub
  (let [directory (m/mk-directory)
        server (m/mk-server "source-1" 1 directory)
        sourcesc (watch-sources directory "test-topic" :last)]
    (publish server "test-topic" "Hello World")
    (Thread/sleep 100) ; Wait for the message to be put in the cache and dropped by the mult.
    (let [uri (:uri (first (a/alts!! [sourcesc (a/timeout 1000)])))
          subc (m/subscribe uri "test-topic" 1)]
      (is uri)
      (is (= (first (a/alts!! [subc (a/timeout 1000)])) "Hello World"))
      (publish server "test-topic" "Hi again")
      (is (= (first (a/alts!! [subc (a/timeout 1000)])) "Hi again"))
      (done server "test-topic")
      (is (= (second (a/alts!! [subc (a/timeout 1000)])) subc)))
    (m/stop-server server)))
