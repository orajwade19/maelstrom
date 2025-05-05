(ns maelstrom.workload.orset.client
  (:require [maelstrom
             [client :as c]
             [net :as net]]
            [maelstrom.workload.orset.protocol :refer [add! read delete!]]
            [jepsen.client :as client]))


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