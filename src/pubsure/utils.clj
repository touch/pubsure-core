(ns pubsure.utils
  "Utility functions for pubsure.")


(defmacro with-debug
  "A debug macro, which keeps the `pre` forms iff the
  *compiler-options* map contains a set under the keyword :with-debug
  holding the given `name` symbol. Any pre form that is not a list,
  will be wrapped with a `prn` call. The body has an implicet do. For
  example:

  (binding [*compiler-options* '{:with-debug #{pubsure}}]
    (eval '(with-debug pubsure
             [(println \"INPUT:\")
              input]
             (work-on input))))"
  [name pre & body]
  (if (get-in *compiler-options* [:with-debug name])
    (let [pre (map (fn [form] (if-not (list? form) `(prn ~form) form)) pre)]
      `(do ~@pre ~@body))
    `(do ~@body)))


(defn conj-set
  "Ensures a conj results in a set."
  [coll val]
  (set (conj coll val)))
