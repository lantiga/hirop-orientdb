(ns hirop-orientdb.core
  (:use hirop.backend)
  (:require [clojure.string :as string]
            [clj-orient.core :as ocore]
            [clj-orient.graph :as ograph]
            [clj-orient.query :as oquery]))

;;TODO: here we should modify the var in a thread-local way.
;; The alternative is to pass connection-data around, which is not bad, after all.
(def ^:dynamic *connection-data* nil)

(defn create-db-if-needed! []
  (when-not (ocore/db-exists? (:connection-string *connection-data*))
    (ocore/create-db! (:connection-string *connection-data*))))

(defn init [connection-data]
  (alter-var-root #'*connection-data* (fn [_] connection-data))
  (try
    (create-db-if-needed!)
    (catch Exception e (prn e))))

(defn hirop-id [odoc]
  (str (:#rid odoc)))

(defn hirop-rev [odoc]
  (str (:#version odoc)))

(defn ->rid [hid]
  (com.orientechnologies.orient.core.id.ORecordId. hid))

(defn orient-id [sdoc]
  (->rid (:_id sdoc)))

(defn orient-rev [sdoc]
  (read-string (:_rev sdoc)))

(defn rel-keys [sdoc]
  (for [[k _] sdoc :when (re-find #"(.*)_$" (name k))] k))

(defn flatten-rels
  ([odoc]
     (flatten-rels odoc nil))
  ([odoc context-name]
     (reduce
      (fn [res edge]
        (if (or (nil? context-name) (= (:context edge) (name context-name)))
          (let [out-vert (ograph/get-vertex edge :in)
                out-id (hirop-id out-vert)
                cardinality (:cardinality edge)
                key (if (nil? context-name)
                      (keyword (str (:context edge) "_" (:field edge)))
                      (keyword (:field edge)))]
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
           rid (str (:#rid odoc))
           version (str (:#version odoc))
           rels (if (= (odoc :#class) :OGraphVertex) (flatten-rels odoc context-name) {})]
       (merge
        sdoc
        {:_id rid :_rev version}
        rels))))

(defn hist->hirop
  [odoc]
  (let [odoc-map (ocore/doc->map odoc)
        sdoc (dissoc odoc-map :#rid :#version :in :out)
        rid (str (:#rid odoc))
        version (str (:#version odoc))]
    (merge
     sdoc
     {:_hid rid :_hrev version})))

(defn orient->hist
  [odoc]
  (orient->hirop odoc))

(defn is-temporary-id?
  [id]
  (string? (re-find #"^tmp" id)))

(defn has-temporary-id?
  [sdoc]
  (is-temporary-id? (:_id sdoc)))

(defn hirop->orient-map
  [sdoc]
  (->
   sdoc
   (#(apply (partial dissoc %) (rel-keys sdoc)))
   (#(dissoc % :_id :_rev))))

(defmacro with-db [& forms]
  `(ocore/with-db
     (ograph/open-graph-db!
      (:connection-string *connection-data*)
      (:username *connection-data*)
      (:password *connection-data*))
     (do ~@forms)))

(defmethod fetch :orientdb
  [backend context-name external-ids boundaries]
  (let [targets (vals external-ids)
        boundaries (map #(str "'" (name %) "'") (filter #(not (contains? external-ids %)) boundaries))
        context-name (name context-name)
        q
        (if (empty? boundaries)
          (str "SELECT FROM (TRAVERSE V.in,E.out,V.out,E.in FROM [" (string/join ", " targets) "] WHERE (@class = 'OGraphVertex' OR context = '" context-name "')) WHERE @class = 'OGraphVertex'")
          (str "SELECT FROM (TRAVERSE V.out, E.in FROM (TRAVERSE V.in,E.out,V.out,E.in FROM [" (string/join ", " targets) "] WHERE ((@class = 'OGraphVertex' AND NOT (_type IN [" (string/join ", " boundaries) "])) OR context = '" context-name "')) WHERE ($depth < 3 AND (@class = 'OGraphVertex' OR context = '" context-name "'))) WHERE @class = 'OGraphVertex'"))]
    (with-db
      (doall
       (map #(orient->hirop % context-name)
            (oquery/sql-query q))))))

(defn query
  [type query & {class-name :class-name context-name :context-name bindings :bindings}]
  (let [class-name (or class-name :OGraphVertex)]
    (with-db
      (doall
       (map #(orient->hirop % context-name)
            (condp = type
                :clj (oquery/clj-query query bindings)
                :sql (oquery/sql-query query bindings)
                :native (oquery/native-query class-name query)))))))

(defn save-document
  [sdoc]
  (with-db
    (let [odoc
          (if (has-temporary-id? sdoc)
            (ograph/vertex)
            (ocore/load (orient-id sdoc)))
          odoc (merge odoc (hirop->orient-map sdoc))]
      (ocore/save! odoc)
      (hirop-id odoc))))

(defn load-document
  [hid]
  (with-db
    (ocore/load (->rid hid))))

(defn save-record-bytes!
  [source]
  (with-db
    (let [rb (ocore/record-bytes source)]
      (ocore/save! rb)
      (str (.getIdentity rb)))))

(defn load-record-bytes
  [hid]
  (with-db
    (let [rb (.load ocore/*db* (->rid hid))]
      (ocore/->bytes rb))))

(defmethod history :orientdb
  [backend id]
  (with-db
    (doall
     (map hist->hirop
          (oquery/sql-query (str "SELECT FROM History WHERE _id = " id))))))

(defn class-name
  [odoc]
  (ocore/oclass-name->kw (.field odoc "@class")))

(defn traverse
  [fields targets pred & [limit]]
  (with-db
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
  [location]
  (try
    (let [db (->
              (com.orientechnologies.orient.core.db.graph.OGraphDatabase. (:connection-string *connection-data*))
              (.open (:username *connection-data*) (:password *connection-data*)))
          listener (reify com.orientechnologies.orient.core.command.OCommandOutputListener
                          (onMessage [this text] nil))
          exporter (com.orientechnologies.orient.core.db.tool.ODatabaseExport. db location listener)]
      (.exportDatabase exporter)
      (.close exporter)
      (.close db))
    (catch Exception e
      (prn "Backup exception:" e))))

(defmethod save :orientdb
  ([backend sdocs]
     (save backend sdocs nil))
  ([backend sdocs context-name]
     (with-db
       (if-not (ocore/exists-class? :History)
         (ocore/create-class! :History))
       (try
         (let [odoc-map
               (ocore/with-tx
                 (let [odoc-map (into {}
                                      (map
                                       (fn [sdoc]
                                         (let [odoc
                                               (if (has-temporary-id? sdoc)
                                                 (ograph/vertex)
                                                 (ocore/load (orient-id sdoc)))]
                                           [(:_id sdoc) odoc]))
                                       sdocs))]
                   ;; any conflicts?
                   (if (empty?
                        (filter
                         (fn [sdoc]
                           (and
                            (get-in odoc-map [(:_id sdoc) :#version])
                            (> (get-in odoc-map [(:_id sdoc) :#version]) (orient-rev sdoc)))) 
                         sdocs))
                     (do
                       ;; ok, no conflict so far, so go ahead, copy old data to history, merge new data, clean old links for the context and create new links for the context
                       ;; TODO: make history optional
                       (doseq [[id odoc] (filter #(not (is-temporary-id? (first %))) odoc-map)]
                         (let [hist-odoc (ocore/document :History)
                               hist-odoc (merge hist-odoc (orient->hist odoc))]
                           (ocore/save! hist-odoc)))
                       (let [odoc-map (into {}
                                            (map
                                             (fn [sdoc]
                                               (let [odoc (get odoc-map (:_id sdoc))
                                                     odoc (merge odoc (hirop->orient-map sdoc))
                                                     out-edges (if context-name
                                                                 (filter #(= (:context %) (name context-name)) (ograph/get-edges odoc :out))
                                                                 [])]
                                                 (doseq [out-edge out-edges]
                                                   (ograph/delete-edge! out-edge))
                                                 [(:_id sdoc) (merge odoc (hirop->orient-map sdoc))]))
                                             sdocs))]
                         ;; create links
                         (doseq [sdoc sdocs]
                           (doseq [rel-key (rel-keys sdoc)]
                             (let [from-odoc (get odoc-map (:_id sdoc))
                                   to-hids (get sdoc rel-key)
                                   rel-data {:field (name rel-key) :context (name context-name) :cardinality (if (coll? to-hids) :many :one)}
                                   to-hids (if (coll? to-hids) to-hids [to-hids])]
                               (doseq [to-hid to-hids]
                                 (let[to-odoc (get odoc-map to-hid)
                                      ;; in case it is an external id, we have to load it
                                      to-odoc (or to-odoc (ocore/load (->rid to-hid)))]
                                   (ograph/link! from-odoc rel-data to-odoc))))))
                         ;; save all
                         (doseq [[_ odoc] odoc-map] (ocore/save! odoc))
                         odoc-map))
                     (throw (Exception. "Conflict detected")))))
               tmp-map
               (into {}
                     (doall
                      (map
                       (fn [el] [(first el) (hirop-id (second el))])
                       (filter #(is-temporary-id? (first %)) odoc-map))))]
           {:result :success :remap tmp-map})
         (catch Exception e
           {:result :conflict})))))
;; TODO: do a better job in identifying the kind of exception and only return :conflict in that case
  