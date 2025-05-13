(ns maelstrom.workload.orset.checker
  "A simple checker for OR-Set eventual consistency.
   Checks that:
   1. The final reads across all nodes are consistent
   2. The final state reflects all completed add/delete operations
   3. Final values are unique (no duplicates in reads)"

  (:require [jepsen [checker :as checker]]
            [maelstrom.net]))


(defn orset-checker
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
                       :delete (if-let [event-ids (:event_id op)]
                                 ; Handle event_id as either a single ID or a collection
                                 (let [event-ids (if (coll? event-ids) event-ids [event-ids])]
                                   ; Only add non-empty event IDs
                                   (if (seq event-ids)
                                     (update state :deletes into event-ids)
                                     state))
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