(ns messages.server
  (:require
   [messages.protocol :as protocol]
   [manifold.stream :as s]
   [byte-streams :as bs]
   [aleph.udp :as udp]))

(defn- fanout [body subs server-socket]
  (map #(s/put! server-socket
                {:host (:host %)
                 :port (:port %)
                 :message body}) subs))

(defn- add-subscriber [subject host port subscribers]
  (if-not (@subscribers subject)
    (swap! subscribers assoc subject #{{:host host :port port}})
    (swap! subscribers update-in [subject] conj {:host host :port port})))

(def server-port 50050)

(defn- dispatch-incoming [subscribers server-socket]
  (fn [{:keys [host port message]}]
    (let [protocol (protocol/get-protocol message)
          subject  (protocol/get-subject message)]
      (case protocol
        :publish
        (fanout message (@subscribers subject) server-socket)
        :subscribe
        (add-subscriber subject host port subscribers)
        :unsubscribe
        (swap! subscribers update-in [subject] disj {:host host :port port})))))

(defn start-udp-server []
  (let [subscribers   (atom {})
        server-socket @(udp/socket {:port server-port})]
    ((-> (dispatch-incoming subscribers server-socket)
         (s/consume server-socket))
     server-socket)))
