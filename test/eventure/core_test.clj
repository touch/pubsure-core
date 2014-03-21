(ns eventure.core-test
  (:require [clojure.test :refer :all]
            [eventure.core :refer :all]
            [eventure.protocols :as p]
            [eventure.memory :as m]
            [clojure.core.async :as a])
  (:import [java.net URI]))


(deftest registry
  (let [registry (m/->MemoryRegistry)]

    ;; Register sources without watchers.
    (p/set-source registry "test-topic" (URI. "memory://source-1"))
    (p/set-source registry "test-topic" (URI. "memory://source-2"))

    (let [sourcesc (p/watch-source registry "test-topic" :last)
          [val _] (a/alts!! [sourcesc (a/timeout 1000)])]
      ;; Test it only got the last set source.
      (is (= val (p/->SourceUpdate "test-topic" (URI. "memory://source-2") :joined)))

      ;; Test is receives subsequent updates.
      (p/unset-source registry "test-topic" (URI. "memory://source-1"))
      (let [[val _] (a/alts!! [sourcesc (a/timeout 1000)])]
        (is (= val (p/->SourceUpdate "test-topic" (URI. "memory://source-1") :left)))))

    ;; Test a new watcher receiving all registered sources.
    (p/set-source registry "test-topic" (URI. "memory://source-1"))
    (let [sourcesc (p/watch-source registry "test-topic" :all)
          [val1 _] (a/alts!! [sourcesc (a/timeout 1000)])
          [val2 _] (a/alts!! [sourcesc (a/timeout 1000)])
          vals (hash-set val1 val2)]
      (is (get vals (p/->SourceUpdate "test-topic" (URI. "memory://source-1") :joined)))
      (is (get vals (p/->SourceUpdate "test-topic" (URI. "memory://source-2") :joined))))))


(deftest no-source-no-wait
  (let [registry (m/->MemoryRegistry)
        chan (subscribe registry "test-topic" :wait-for-source 0)
        [_ port] (a/alts!! [chan (a/timeout 100)])]
    (is (= port chan))))


(deftest no-source-wait
  (let [registry (m/->MemoryRegistry)
        chan (subscribe registry "test-topic" :wait-for-source 2000)
        [_ port1] (a/alts!! [chan (a/timeout 1900)])
        [_ port2] (a/alts!! [chan (a/timeout 200)])]
    (is (not= port1 chan))
    (is (= port2 chan))))


(deftest write-and-read
  (let [registry (m/->MemoryRegistry)
        server (m/mk-memory-server "source-1" registry)
        chan (subscribe registry "test-topic" :wait-for-source 2000)]
    (publish server "test-topic" "Hello World")
    (is (= (first (a/alts!! [chan (a/timeout 500)])) "Hello World"))
    (publish server "test-topic" "Hi again")
    (is (= (first (a/alts!! [chan (a/timeout 500)])) "Hi again"))
    (is (deref (unsubscribe "test-topic") 1000 false) "failed to unsubscribe")
    (p/stop server)))


(deftest mix-on-join
  (let [registry (m/->MemoryRegistry)
        server1 (m/mk-memory-server "source-1" registry)
        server2 (m/mk-memory-server "source-2" registry)
        chan (subscribe registry "test-topic" :wait-for-source 2000 :on-source-join :mix)]
    (publish server1 "test-topic" "Hello World")
    (publish server2 "test-topic" "Hi again")
    (is (= (first (a/alts!! [chan (a/timeout 500)])) "Hello World"))
    (is (= (first (a/alts!! [chan (a/timeout 500)])) "Hi again"))
    (is (deref (unsubscribe "test-topic") 1000 false) "failed to unsubscribe")
    (p/stop server1)
    (p/stop server2)))


(deftest last-on-join
  (let [registry (m/->MemoryRegistry)
        server1 (m/mk-memory-server "source-1" registry)
        server2 (m/mk-memory-server "source-2" registry)
        chan (subscribe registry "test-topic" :wait-for-source 2000 :on-source-join :last)]
    (publish server1 "test-topic" "Hello World")
    (Thread/sleep 1000)
    (publish server2 "test-topic" "Hi again")
    (is (= (first (a/alts!! [chan (a/timeout 500)])) "Hello World"))
    (is (= (first (a/alts!! [chan (a/timeout 500)])) "Hi again"))
    (is (deref (unsubscribe "test-topic") 1000 false) "failed to unsubscribe")
    (p/stop server1)
    (p/stop server2)))


(deftest first-on-join
  (let [registry (m/->MemoryRegistry)
        server1 (m/mk-memory-server "source-1" registry)
        server2 (m/mk-memory-server "source-2" registry)
        chan (subscribe registry "test-topic" :wait-for-source 2000 :on-source-join :first)]
    (publish server1 "test-topic" "Hello World")
    (Thread/sleep 1000)
    (publish server2 "test-topic" "Hi again")
    (is (= (first (a/alts!! [chan (a/timeout 500)])) "Hello World"))
    (is (= (first (a/alts!! [chan (a/timeout 500)])) nil))
    (is (deref (unsubscribe "test-topic") 1000 false) "failed to unsubscribe")
    (p/stop server1)
    (p/stop server2)))
