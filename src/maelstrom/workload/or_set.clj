(ns maelstrom.workload.or-set
  "An or-set workload: clients can add and delete elements to a set, and read the
  current value of the set."
  (:refer-clojure :exclude [read])
  (:require [maelstrom 
             [net :as net]
             [nemesis :as custom-nemesis]]
            [jepsen [checker :as checker]
             [generator :as gen]
             [nemesis :as nemesis]]
            [schema.core :as s] 
            [clojure.tools.logging :refer [info warn error debug]]
            [maelstrom.workload.orset.checker :as orset-custom-checker]
            [maelstrom.workload.orset.nemesis :as orset-custom-nemesis]
            [maelstrom.workload.orset.client :as orset-custom-client]))

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
     ;;  (gen/once {:f :delete, :value test-value, :process 1})

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


(defn workload
  "Constructs a workload for a grow-only set, given options from the CLI
  test constructor:

      {:net     A Maelstrom network}"
  [opts]
  {:client    (orset-custom-client/client (:net opts))
   :nemesis (nemesis/partitioner orset-custom-nemesis/isolate-node-0)
   :generator (orset-partition-test-generator)
   :nemesis-final-generator (gen/each-thread {:type :info, :f :stop})
   :final-generator (gen/clients
                     (gen/each-process
                      (gen/once {:f :read})))
   :checker   (orset-custom-checker/orset-checker)})
