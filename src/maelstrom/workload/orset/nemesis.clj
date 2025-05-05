(ns maelstrom.workload.orset.nemesis
  "Custom fault injection for the OR-Set workload."
  (:require [jepsen [nemesis :as nc]]))


(defn isolate-node-0
  "Creates a grudge that isolates node n0 from the rest"
  [nodes]
  (let [[part1 part2] (nc/split-one (first nodes) nodes)]
    (nc/complete-grudge [part1 part2])))
