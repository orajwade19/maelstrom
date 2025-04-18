(ns maelstrom.workload.or-set-perf
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





(defn orset-perf
  "Random mix of operations with rate control and time limit"
  []
  (gen/clients
   (gen/mix [(->> (range) (map (fn [x] {:f :add, :value x})))
             (repeat {:f :read})
             ;; (->> (range) (map (fn [x] {:f :delete, :value x})))
             ])))

(defn isolate-node-0
  "Creates a grudge that isolates node n0 from the rest"
  [nodes]
  (let [[part1 part2] (nemesis/split-one (first nodes) nodes)]
    (nemesis/complete-grudge [part1 part2])))



(defn orset-checker
  "A simple checker for OR-Set eventual consistency.
   Checks that:
   1. The final reads across all nodes are consistent
   2. The final state reflects all completed add/delete operations
   3. Final values are unique (no duplicates in reads)"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [; Get only completed client operations
            client-ops (filter #(and (not= :nemesis (:process %))
                                     (= :ok (:type %)))
                               history)

            ; Track adds and deletes by their event IDs
            state (reduce
                   (fn [state op]
                     (case (:f op)
                       :add   (assoc-in state [:adds (:value op)] (:event_id op))
                       :delete (if (and (:event_id op) (not= "" (:event_id op)))
                                 (update state :deletes conj (:event_id op))
                                 state)
                       :read  (update state :reads conj
                                      {:time (:time op)
                                       :process (:process op)
                                       :values (set (:value op))
                                       :raw-values (:value op)}) ; Keep raw values to check for duplicates
                       state))
                   {:adds {}      ; map of value -> event_id
                    :deletes #{}  ; set of deleted event_ids
                    :reads []}    ; sequence of reads
                   client-ops)

            ; Get the final read from each process
            final-reads (->> (:reads state)
                             (sort-by :time)
                             (group-by :process)
                             (map (fn [[process reads]]
                                    (let [last-read (last reads)]
                                      [process {:values (:values last-read)
                                                :raw-values (:raw-values last-read)}])))
                             (into {}))

            ; Calculate expected set by filtering out deleted elements
            expected-values (->> (:adds state)
                                 (filter (fn [[_ event-id]]
                                           (not (contains? (:deletes state) event-id))))
                                 (map first)
                                 (set))

            ; Check if all nodes see the same values in their final read
            reads-consistent? (= 1 (count (distinct (map :values (vals final-reads)))))

            ; Check for duplicates in the final reads
            duplicate-values (->> final-reads
                                  (map (fn [[process read-data]]
                                         (let [raw-values (:raw-values read-data)
                                               duplicates (when raw-values
                                                            (->> raw-values
                                                                 (frequencies)
                                                                 (filter (fn [[val freq]] (> freq 1)))
                                                                 (map first)))]
                                           [process duplicates])))
                                  (filter (fn [[_ dups]] (seq dups)))
                                  (into {}))

            has-duplicates? (not (empty? duplicate-values))

            ; If consistent, check if they match expected values
            correct-values? (or (not reads-consistent?)
                                (= expected-values (first (distinct (map :values (vals final-reads))))))]

        {:valid? (and reads-consistent? correct-values? (not has-duplicates?))
         :reads-consistent? reads-consistent?
         :correct-values? correct-values?
         :expected-values expected-values
         :final-reads (into {} (map (fn [[k v]] [k (:values v)]) final-reads))
         :has-duplicates? has-duplicates?
         :duplicate-values duplicate-values
         :tracking {:adds (:adds state)
                    :deletes (:deletes state)}}))))



(defn workload
  "Constructs a workload for a grow-only set, given options from the CLI
  test constructor:

      {:net     A Maelstrom network}"
  [opts]
  {:client    (client (:net opts))
   ;;  :nemesis (nemesis/partitioner isolate-node-0)
   :generator (orset-perf)
   ;;  :nemesis-final-generator (gen/once {:type :info, :f :heal})
   :final-generator (gen/each-thread {:f :read})
   :checker   (orset-checker)})