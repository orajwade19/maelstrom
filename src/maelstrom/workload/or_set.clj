(ns maelstrom.workload.or-set
  "A grow-only set workload: clients add elements to a set, and read the
  current value of the set."
  (:refer-clojure :exclude [read])
  (:require [maelstrom [client :as c]
             [net :as net]]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [schema.core :as s]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen.generator :as gen]))

;; Append-only log for storing event IDs
(def event-log (atom []))

(defn store-event! [event-id operation element]
  "Stores the event_id along with operation details."
  (swap! event-log conj {:event_id event-id
                         :operation operation
                         :element element}))


(c/defrpc add!
  "Requests that a server add a single element to the set."
  {:type    (s/eq "add")
   :element s/Any}
  {:type    (s/eq "add_ok")
   :event_id s/Str}) ;; Ensure event_id is part of response

(c/defrpc delete!
  "Requests that a server delete a single element from the set."
  {:type    (s/eq "delete")
   :element s/Any}
  {:type    (s/eq "delete_ok")
   :event_id s/Str}) ;; Ensure event_id is part of response

(c/defrpc read
  "Requests the current set of all elements. Servers respond with a message
  containing an `elements` key, whose `value` is a JSON array of added
  elements."
  {:type (s/eq "read")}
  {:type (s/eq "read_ok")
   :value [s/Any]})

(defn client
  ([net]
   (client net nil nil))
  ([net conn node]
   (reify client/Client
     (open! [this test node]
       (client net (c/open! net) node))

     (setup! [this test])

     (invoke! [_ test op]
       (case (:f op)
         :add (let [resp (add! conn node {:element (:value op)})]
                (when-let [event-id (:event_id resp)] ;; Extract event_id if present
                  (store-event! event-id "add" (:value op)))
                (assoc op :type :ok))
         :read (assoc op
                      :type :ok
                      :value (:value (read conn node {})))
         :delete (let [resp (delete! conn node {:element (:value op)})]
                   (when-let [event-id (:event_id resp)] ;; Extract event_id if present
                     (store-event! event-id "delete" (:value op)))
                   (assoc op :type :ok))))

     (teardown! [_ test])

     (close! [_ test]
       (c/close! conn)))))

(defn workload
  "Constructs a workload for a grow-only set, given options from the CLI
  test constructor:

      {:net     A Maelstrom network}"
  [opts]
  {:client    (client (:net opts))
   :generator (gen/then (gen/mix [(->> (range) (map (fn [x] {:f :delete, :value x})))
                                  (repeat {:f :read})])
                        (gen/mix [(->> (take 10 (range)) (map (fn [x] {:f :add, :value x})))
                                  (take 20 (repeat {:f :read}))]))
   :final-generator (gen/each-thread {:f :read})
   :checker   (checker/unbridled-optimism)})
