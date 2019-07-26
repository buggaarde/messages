(ns messages.client
  (:require [manifold.stream :as s]
            [aleph.udp :as udp]
            [messages.protocol :as protocol]))

(defn- get-free-port []
  (let [socket (java.net.ServerSocket. 0)]
    (.close socket)
    (.getLocalPort socket)))

(defn attatch-to [server-socket]
  (let [port (get-free-port)]
    (fn [name]
      {:server-socket server-socket
       :name   name
       :socket @(udp/socket {:port (get-free-port)})
       :host   "localhost"
       :port   (get-free-port)})))

(defn subscribe [client subject]
  (s/put! (:socket client)
          {:host    "localhost"
           :port    50050
           :message (protocol/to-bytes (protocol/SUB subject))}))

(defn unsubscribe [client subject]
  (s/put! (:socket client)
          {:host    "localhost"
           :port    50050
           :message (protocol/to-bytes (protocol/UNSUB subject))}))

(defn publish [client subject payload]
  (s/put! (:socket client)
          {:host    "localhost"
           :port    50050
           :message (protocol/to-bytes (protocol/PUB subject payload))}))
