(ns maelstrom.workload.or-set-perf
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


(defn orset-perf
  "Random mix of operations with rate control and time limit"
  []
  (gen/clients
   (gen/mix [(->> (range) (map (fn [x] {:f :add, :value x})))
             (repeat {:f :read})
             ;; (->> (range) (map (fn [x] {:f :delete, :value x})))
             ])))

(defn workload
  "Constructs a workload for a grow-only set, given options from the CLI
  test constructor:

      {:net     A Maelstrom network}"
  [opts]
  {:client    (orset-custom-client/client (:net opts))
   ;;  :nemesis (nemesis/partitioner isolate-node-0)
   :generator (orset-perf)
   ;;  :nemesis-final-generator (gen/once {:type :info, :f :heal})
   :final-generator (gen/each-thread {:f :read})
   :checker   (orset-custom-checker/orset-checker)})