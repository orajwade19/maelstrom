(ns maelstrom.workload.or-set-repeat
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

(defn orset-repeat-workload
  "Random mix of add and delete operations on the same constant value from client processes"
  []
  (gen/time-limit
   5
   (gen/clients
    (gen/mix [(repeat {:f :add, :value 42})
              (repeat {:f :read})
              (repeat {:f :delete, :value 42})]))))

(defn workload
  "Constructs a workload for a grow-only set, given options from the CLI
  test constructor:

      {:net     A Maelstrom network}"
  [opts]
  {:client    (orset-custom-client/client (:net opts))
   :nemesis (nemesis/partitioner orset-custom-nemesis/isolate-node-0)
   :generator (orset-repeat-workload)
   :nemesis-final-generator (gen/each-thread {:type :info, :f :stop})
   :final-generator (gen/clients
                     (gen/each-process
                      (gen/once {:f :read})))
   :checker   (orset-custom-checker/orset-checker)})
