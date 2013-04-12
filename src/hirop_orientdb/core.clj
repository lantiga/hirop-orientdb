(ns hirop-orientdb.core
  (:use hirop.backend)
  (:require [hirop.core :as hirop]
            [clojure.string :as string]
            [clj-orient.core :as ocore]
            [clj-orient.graph :as ograph]
            [clj-orient.query :as oquery]))

(defmacro with-db [backend & forms]
  `(ocore/with-db
     (ograph/open-graph-db!
      (:connection-string ~backend)
      (:username ~backend)
      (:password ~backend))
     (do ~@forms)))

(defn init-database
  [connection-data]
  (try
    (when-not (ocore/db-exists? (:connection-string connection-data))
      (ocore/create-db! (:connection-string connection-data)))
    (catch Exception e (prn e))))

(defn hirop-id [odoc]
  #_(str (:#rid odoc))
  (:_hirop_id odoc))

(defn hirop-rev [odoc]
  (str (:#version odoc)))

(defn ->rid [oid]
  (com.orientechnologies.orient.core.id.ORecordId. oid))

(defn orient-id [sdoc]
  (->rid (get-in sdoc [:_hirop :orient-id])))

(defn assoc-orient-id [sdoc oid]
  (assoc-in sdoc [:_hirop :orient-id] oid))

(defn orient-rev [sdoc]
  (read-string (hirop/hrev sdoc)))

(defn rel-keys [sdoc]
  (keys (hirop/hrels sdoc)))

(defn flatten-rels
  ([odoc]
     (flatten-rels odoc nil))
  ([odoc context-name]
     (reduce
      (fn [res edge]
        (if (or (nil? context-name) (= (:context edge) (name context-name)))
          (let [out-vert (ograph/get-vertex edge :in)
                out-id (hirop-id out-vert)
                cardinality (keyword (:cardinality edge))
                key (if (nil? context-name)
                      (keyword (str (:context edge) "_" (:_hirop_type out-vert)))
                      (keyword (:_hirop_type out-vert)))]
            (condp = cardinality
              :one (assoc res key out-id)
              :many (update-in res [key] #(conj (or % []) out-id))))
          res))
      {}
      (ograph/get-edges odoc :out))))

(defn orient->hirop
  ([odoc]
     (orient->hirop odoc nil))
  ([odoc context-name]
     (let [odoc-map (ocore/doc->map odoc)
           sdoc (dissoc odoc-map :#rid :#version :in :out)
           sdoc (dissoc sdoc :_hirop_id :_hirop_conf :_hirop_meta :_hirop_type)
           rid (str (:#rid odoc))
           version (str (:#version odoc))
           rels (if (= (odoc :#class) :OGraphVertex) (flatten-rels odoc context-name) {})]
       (->
        sdoc 
        (assoc-orient-id rid) 
        (hirop/assoc-hid (:_hirop_id odoc))
        (hirop/assoc-hrev version) 
        (hirop/assoc-hrels rels)
        (hirop/assoc-hconf (:_hirop_conf odoc))
        (hirop/assoc-hmeta (:_hirop_meta odoc))
        (hirop/assoc-htype (:_hirop_type odoc))))))

(defn hist->hirop
  [odoc]
  (let [odoc-map (ocore/doc->map odoc)
        sdoc (dissoc odoc-map :#rid :#version :in :out)]
    sdoc))

(defn orient->hist
  [odoc]
  (let [sdoc (orient->hirop odoc)]
    (assoc sdoc :_hirop_id (hirop/hid sdoc))))

(defn hirop->orient-map
  [sdoc]
  (->
   sdoc
   (dissoc :_hirop)
   (assoc :_hirop_id (hirop/hid sdoc))
   (assoc :_hirop_conf (hirop/hconf sdoc))
   (assoc :_hirop_meta (hirop/hmeta sdoc))
   (assoc :_hirop_type (name (hirop/htype sdoc)))))


(defn query
  [backend type query & {class-name :class-name context-name :context-name bindings :bindings}]
  (let [class-name (or class-name :OGraphVertex)]
    (with-db backend
      (doall
       (map #(orient->hirop % context-name)
            (condp = type
              :clj (oquery/clj-query query bindings)
              :sql (oquery/sql-query query bindings)
              :native (oquery/native-query class-name query)))))))

(defn save-document
  [backend sdoc]
  (with-db backend
    (let [odoc
          (if (orient-id sdoc)
            (ograph/vertex)
            (ocore/load (orient-id sdoc)))
          odoc (merge odoc (hirop->orient-map sdoc))]
      (ocore/save! odoc)
      (hirop-id odoc))))

(defn load-document
  [backend orient-id]
  (with-db backend
    (ocore/load (->rid orient-id))))

(defn save-record-bytes!
  [backend source]
  (with-db backend
    (let [rb (ocore/record-bytes source)]
      (ocore/save! rb)
      (str (.getIdentity rb)))))

(defn load-record-bytes
  [backend orient-id]
  (with-db backend
    (let [rb (.load ocore/*db* (->rid orient-id))]
      (ocore/->bytes rb))))

(defn class-name
  [odoc]
  (ocore/oclass-name->kw (.field odoc "@class")))

(defn traverse
  [backend fields targets pred & [limit]]
  (with-db backend
    (let [targets
          (map
           (fn [target]
             (cond
              (ocore/orid? target) target
              (ocore/document? target) target
              :else (->rid target)))
           targets)]
      (doall
       (oquery/traverse fields targets pred limit)))))

(defn backup
  [connection-data location]
  (try
    (let [db (->
              (com.orientechnologies.orient.core.db.graph.OGraphDatabase. (:connection-string connection-data))
              (.open (:username connection-data) (:password connection-data)))
          listener (reify com.orientechnologies.orient.core.command.OCommandOutputListener
                     (onMessage [this text] nil))
          exporter (com.orientechnologies.orient.core.db.tool.ODatabaseExport. db location listener)]
      (.exportDatabase exporter)
      (.close exporter)
      (.close db))
    (catch Exception e
      (prn "BACKUP EXCEPTION:" e)
      (throw e))))

(defn save*
  [backend sdocs context-name]
  (with-db backend
    (if-not (ocore/exists-class? :History)
      (ocore/create-class! :History))
    (try
      (let [odoc-map
            (ocore/with-tx
              (let [odoc-map (into {}
                                   (map
                                    (fn [sdoc]
                                      (let [odoc
                                            (if (orient-id sdoc)
                                              (ograph/vertex)
                                              (ocore/load (orient-id sdoc)))]
                                        [(hirop/hid sdoc) odoc]))
                                    sdocs))]
                ;; any conflicts?
                (if (empty?
                     (filter
                      (fn [sdoc]
                        (and
                         (get-in odoc-map [(hirop/hid sdoc) :#version])
                         (> (get-in odoc-map [(hirop/hid sdoc) :#version]) (orient-rev sdoc)))) 
                      sdocs))
                  (do
                    ;; ok, no conflict so far, so go ahead, copy old data to history, merge new data, clean old links for the context and create new links for the context
                    ;; TODO: make history optional
                    (doseq [[id odoc] odoc-map]
                      (let [hist-odoc (ocore/document :History)
                            hist-odoc (merge hist-odoc (orient->hist odoc))]
                        (ocore/save! hist-odoc)))
                    (let [odoc-map (into {}
                                         (map
                                          (fn [sdoc]
                                            (let [odoc (get odoc-map (hirop/hid sdoc))
                                                  odoc (merge odoc (hirop->orient-map sdoc))
                                                  out-edges (if context-name
                                                              (filter #(= (:context %) (name context-name)) (ograph/get-edges odoc :out))
                                                              [])]
                                              (doseq [out-edge out-edges]
                                                (ograph/delete-edge! out-edge))
                                              [(hirop/hid sdoc) (merge odoc (hirop->orient-map sdoc))]))
                                          sdocs))]
                      ;; create links
                      (doseq [sdoc sdocs]
                        (doseq [rel-key (rel-keys sdoc)]
                          (let [from-odoc (get odoc-map (hirop/hid sdoc))
                                to-hids (hirop/hrel sdoc rel-key)
                                rel-data {:context (name context-name) :cardinality (if (coll? to-hids) "many" "one")}
                                to-hids (if (coll? to-hids) to-hids [to-hids])]
                            (doseq [to-hid to-hids]
                              (let[to-odoc (get odoc-map to-hid)
                                   ;; in case it is an external id, we have to load it
                                   ;; FIXME: we have to load it by oid, not hid
                                   to-odoc (or to-odoc (ocore/load (->rid to-hid)))]
                                (ograph/link! from-odoc rel-data to-odoc))))))
                      ;; save all
                      (doseq [[_ odoc] odoc-map] (ocore/save! odoc))
                      odoc-map))
                  (throw (Exception. "Conflict detected")))))]
        {:result :success})
      (catch Exception e
        (prn e)
        {:result :conflict}))))
;; TODO: do a better job in identifying the kind of exception and only return :conflict in that case

(defn fetch*
  [backend context-name external-ids boundaries]
  (let [;; FIXME: targets is not correct: we need an INDEX from hids to oids in order to be able to traverse from hids 
        targets (vals external-ids)
        boundaries (distinct (map #(str "'" (name %) "'") (filter #(not (contains? external-ids %)) boundaries)))
        context-name (name context-name)
        q
        (if (empty? boundaries)
          (str "SELECT FROM (TRAVERSE V.in,E.out,V.out,E.in FROM [" (string/join ", " targets) "] WHERE (@class = 'OGraphVertex' OR context = '" context-name "')) WHERE @class = 'OGraphVertex'")
          (str "SELECT FROM (TRAVERSE V.out, E.in FROM (TRAVERSE V.in,E.out,V.out,E.in FROM [" (string/join ", " targets) "] WHERE ((@class = 'OGraphVertex' AND NOT (_hirop_type IN [" (string/join ", " boundaries) "])) OR context = '" context-name "')) WHERE ($depth < 3 AND (@class = 'OGraphVertex' OR context = '" context-name "'))) WHERE @class = 'OGraphVertex'"))
        documents (with-db backend
                           (doall
                             (map #(orient->hirop % context-name)
                                  (oquery/sql-query q))))]
      {:documents documents
       :context-info nil}))


(defmethod fetch :orientdb
  [backend context]
  (fetch* backend (:name context) (:external-ids context) (hirop/get-free-external-doctypes context)))

(defmethod save :orientdb
  [backend context]
  (let [sdocs (vals (:starred context))
        context-name (:name context)]
    (save* backend sdocs context-name)))

(defmethod history :orientdb
  [backend id]
  (with-db backend
    (doall
     (map hist->hirop
          (oquery/sql-query (str "SELECT FROM History WHERE _hirop_id = " id))))))

