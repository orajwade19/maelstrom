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



(defn orset-checker
  "Checks eventual consistency for an OR-Set (Observed-Remove Set) with
   awareness of network partitions.
   
   During a network partition:
   - Nodes may have inconsistent views
   - Reads may contain elements that are added but not visible to all nodes
   - Delete operations may not affect elements not visible to the node
   
   After healing, the system should eventually converge."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [; Get all operations including nemesis events
            ops (vec history)

            ; Find partition start/stop events
            partition-events (->> ops
                                  (filter #(and (= :nemesis (:process %))
                                                (or (= :start (:f %))
                                                    (= :stop (:f %)))))
                                  (map #(assoc % :partition? (= :start (:f %)))))

            ; Create timeline of when partitions are active
            partition-timeline (reduce
                                (fn [timeline event]
                                  (conj timeline {:time (:time event)
                                                  :partition? (:partition? event)}))
                                []
                                partition-events)

            ; Sort timeline by time
            partition-timeline (sort-by :time partition-timeline)

            ; Function to check if a time is during a partition
            during-partition? (fn [time]
                                (let [relevant-events (filter #(<= (:time %) time) partition-timeline)
                                      last-event (last relevant-events)]
                                  (and last-event (:partition? last-event))))

            ; Get client operations
            client-ops (filter #(not= :nemesis (:process %)) ops)

            ; Track element state through the history
            state (reduce
                   (fn [state op]
                     (let [process (:process op)
                           type (:type op)
                           f (:f op)
                           value (:value op)
                           event-id (:event_id op)
                           during-partition (during-partition? (:time op))]
                       (case [type f]
                         ; Add invocation: track pending add
                         [:invoke :add]
                         (update state :pending-adds conj value)

                         ; Add completion: track confirmed add and remove from pending
                         [:ok :add]
                         (-> state
                             (update :pending-adds disj value)
                             (update :added-values conj value)
                             (update :add-events assoc value event-id))

                         ; Delete invocation: track pending delete
                         [:invoke :delete]
                         (update state :pending-deletes conj value)

                         ; Delete completion: track confirmed delete and remove from pending
                         [:ok :delete]
                         (let [; Only mark as deleted if event-id matches and is not empty
                               valid-delete? (and (string? event-id)
                                                  (not= "" event-id)
                                                  (= event-id (get (:add-events state) value)))]
                           (-> state
                               (update :pending-deletes disj value)
                               (update :delete-ops conj {:time (:time op)
                                                         :process process
                                                         :value value
                                                         :event-id event-id
                                                         :during-partition during-partition
                                                         :valid valid-delete?})
                               (update :deleted-values
                                       (if valid-delete?
                                         #(conj % value)
                                         identity))))

                         ; Read completion: record what was read
                         [:ok :read]
                         (update state :reads conj {:time (:time op)
                                                    :process process
                                                    :values (set value)
                                                    :during-partition during-partition})

                         ; Default: return state unchanged
                         state)))
                   {:pending-adds #{}
                    :pending-deletes #{}
                    :added-values #{}
                    :deleted-values #{}
                    :add-events {}
                    :delete-ops []
                    :reads []}
                   client-ops)

            ; Get the last reads during non-partition times for each node
            final-reads (->> (:reads state)
                             (filter (comp not :during-partition))
                             (sort-by :time)
                             (group-by :process)
                             (map (fn [[node reads]] [node (last reads)]))
                             (into {}))

            ; Extract just the values from the final reads
            final-values-by-node (into {}
                                       (map (fn [[node read]]
                                              [node (if read (:values read) #{})])
                                            final-reads))

            ; Check if all final non-partition reads are consistent
            reads-consistent? (= 1 (count (distinct (vals final-values-by-node))))

            ; Get the set of values after the final accepted delete operation
            final-deleted-values (:deleted-values state)
            final-added-values (:added-values state)
            expected-final-set (clojure.set/difference final-added-values final-deleted-values)

            ; Check if there are any valid final reads
            has-final-reads? (not-empty final-values-by-node)

            ; The test is valid if readings are consistent after healing
            ; or if there are no final reads to check
            valid? (or reads-consistent? (not has-final-reads?))]

        {:valid? valid?
         :reads-consistent reads-consistent?
         :expected-final-set expected-final-set
         :final-values-by-node final-values-by-node
         :partition-timeline partition-timeline
         :all-events {:added final-added-values
                      :deleted final-deleted-values
                      :deletion-operations (:delete-ops state)
                      :reads-during-partition (filter :during-partition (:reads state))
                      :reads-after-healing (filter (comp not :during-partition) (:reads state))}
         :debug {:all-reads (:reads state)}}))))
(defn workload
  "Constructs a workload for a grow-only set, given options from the CLI
  test constructor:

      {:net     A Maelstrom network}"
  [opts]
  {:client    (client (:net opts))
   :nemesis (nemesis/partitioner isolate-node-0)
   :generator (orset-partition-test-generator)
   :final-generator (gen/each-thread {:f :read})
   :checker   (orset-checker)})
