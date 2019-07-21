(ns messages.protocol
  (:require [byte-streams :as bs]))

(defn to-bytes [{:keys [protocol subject payload]}]
  (let [pcolb      (protocol protocol->byte)
        subb       (map byte subject)
        data-index (inc (count (conj subb pcolb)))]
    (case protocol
      :publish
      (let [payb (bs/to-byte-array payload)]
        (byte-array (mapcat seq [[data-index] (conj subb pcolb) payb])))
      ;; :subscribe and :unsubscribe
      (byte-array (flatten [data-index (conj subb pcolb)])))))

(defn get-payload [ba]
  (byte-array (nthrest ba (first ba))))

(defn get-protocol [byte-array]
  (byte->protocol (second byte-array)))

(defn get-subject [ba]
  (-> (take (first ba) ba)
      next next
      byte-array))

(def to-byte
  {:subscribe   (byte 1)
   :unsubscribe (byte 2)
   :publish     (byte 3)})

(def from-byte
  (clojure.set/map-invert protocol->byte))

(defn SUB [subject]
  {:protocol :subscribe
   :subject subject})

(defn UNSUB [subject]
  {:protocol :unsubscribe
   :subject subject})

(defn PUB [subject payload]
  {:protocol :publish
   :subject subject
   :payload payload})
