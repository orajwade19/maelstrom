(ns maelstrom.workload.or-set
  "A grow-only set workload: clients add elements to a set, and read the
  current value of the set."
  (:refer-clojure :exclude [read])
  (:require [maelstrom [client :as c]
             [net :as net]]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]
             [nemesis :as nemesis]]
            [schema.core :as s]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen.generator :as gen]
            [clojure.tools.logging :refer [info warn error debug]]))



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
           :add (let [resp (add! conn node {:element (:value op)})
                      op (assoc op :type :ok)
                      op (if-let [event_id (:event_id resp)]
                           (assoc op :event_id event_id)
                           op)]
                  op)

           :read (assoc op
                        :type :ok
                        :value (:value (read conn node {})))

           :delete (let [resp (delete! conn node {:element (:value op)})
                         op (assoc op :type :ok)
                      op (if-let [event_id (:event_id resp)]
                           (assoc op :event_id event_id)
                           op)]
                     op)))

     (teardown! [_ test])

     (close! [_ test]
       (c/close! conn)))))


(defn simple-test-generator []
  (gen/phases
   (gen/log "Testing basic operations")
   (gen/clients
    (gen/each-process
     (gen/phases
      (gen/once {:type :invoke, :f :add, :value 42})
      (gen/sleep 1)
      (gen/once {:type :invoke, :f :read})
      (gen/sleep 1)
      (gen/once {:type :invoke, :f :delete, :value 42})
      (gen/sleep 1)
      (gen/once {:type :invoke, :f :read}))))))

(defn orset-partition-test-generator
  "Creates a partition test generator for ORSet"
  []
  (let [test-value 42]
    (gen/phases

     (gen/log "Creating network partition - isolating node 0")
     (gen/nemesis {:type :info, :f :start})
     (gen/sleep 2)
      ;; Step 2: Add value to node 0
     (gen/log "Adding value to node 0")
     (gen/once {:f :add, :value test-value, :process 0})

      ;; Step 3: Read from node 0
     (gen/log "Reading from node 0")
     (gen/once {:f :read, :process 0})

      ;; Step 4: Read from node 1
     (gen/log "Reading from node 1")
     (gen/once {:f :read, :process 1})

      ;; Step 5: Delete added value from node 1
     (gen/log "Deleting value from node 1")
     (gen/once {:f :delete, :value test-value, :process 1})

      ;; Step 6: Read from node 1
     (gen/log "Reading from node 1 after delete attempt")
     (gen/once {:f :read, :process 1})

      ;; Step 7: Read from node 0
     (gen/log "Reading from node 0 after node 1 delete attempt")
     (gen/once {:f :read, :process 0})

     ;; Step 8: Heal the network partition
     (gen/log "Healing network partition")
     (gen/nemesis {:type :info, :f :stop})
     (gen/sleep 3)


      ;; Wait to ensure nodes synchronize
     (gen/sleep 3)

      ;; Step 9: Delete added value from node 1 again
     (gen/log "Deleting value from node 1 after healing")
     (gen/once {:f :delete, :value test-value, :process 1})

      ;; Step 10: Read from node 1
     (gen/log "Reading from node 1 after healing and delete")
     (gen/once {:f :read, :process 1})

      ;; Step 11: Read from node 0
     (gen/log "Reading from node 0 after healing and delete")
     (gen/once {:f :read, :process 0})

      ;; Final verification reads
     (gen/sleep 2)
     (gen/log "Verification reads from all nodes")
     (gen/clients
      (gen/each-process
       (gen/once {:f :read}))))))


(defn isolate-node-0
  "Creates a grudge that isolates node n0 from the rest"
  [nodes]
  (let [[part1 part2] (nemesis/split-one (first nodes) nodes)]
    (nemesis/complete-grudge [part1 part2])))

(defn workload
  "Constructs a workload for a grow-only set, given options from the CLI
  test constructor:

      {:net     A Maelstrom network}"
  [opts]
  {:client    (client (:net opts))
   :nemesis (nemesis/partitioner isolate-node-0)
   :generator (orset-partition-test-generator)
   :final-generator (gen/each-thread {:f :read})
   :checker   (checker/unbridled-optimism)})
