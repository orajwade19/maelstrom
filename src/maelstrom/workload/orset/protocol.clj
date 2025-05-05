(ns maelstrom.workload.orset.protocol
  (:require [maelstrom [client :as c]]
            [schema.core :as s]))


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
   :event_id [s/Str]}) ;; Ensure event_id is part of response

(c/defrpc read
  "Requests the current set of all elements. Servers respond with a message
  containing an `elements` key, whose `value` is a JSON array of added
  elements."
  {:type (s/eq "read")}
  {:type (s/eq "read_ok")
   :value [s/Any]})
