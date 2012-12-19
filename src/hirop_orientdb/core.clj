(ns hirop-orientdb.core
  (:use hirop.backend)
  (:require [hirop.core :as hirop]
            [clojure.string :as string]
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
  (->rid (hirop/hid sdoc)))

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
           sdoc (dissoc sdoc :_hirop_conf :_hirop_meta :_hirop_type)
           rid (str (:#rid odoc))
           version (str (:#version odoc))
           rels (if (= (odoc :#class) :OGraphVertex) (flatten-rels odoc context-name) {})]
       (->
        sdoc 
        (hirop/assoc-hid rid) 
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
   (assoc :_hirop_conf (hirop/hconf sdoc))
   (assoc :_hirop_meta (hirop/hmeta sdoc))
   (assoc :_hirop_type (name (hirop/htype sdoc)))))

(defmacro with-db [& forms]
  `(ocore/with-db
     (ograph/open-graph-db!
      (:connection-string *connection-data*)
      (:username *connection-data*)
      (:password *connection-data*))
     (do ~@forms)))

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
          (if (hirop/has-temporary-id? sdoc)
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
      (prn "BACKUP EXCEPTION:" e)
      (throw e))))

(defn save*
  [sdocs context-name]
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
                                            (if (hirop/has-temporary-id? sdoc)
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
                    (doseq [[id odoc] (filter #(not (hirop/is-temporary-id? (first %))) odoc-map)]
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
                    (filter #(hirop/is-temporary-id? (first %)) odoc-map))))]
        {:result :success :remap tmp-map})
      (catch Exception e
        (prn e)
        {:result :conflict}))))
;; TODO: do a better job in identifying the kind of exception and only return :conflict in that case

(defn fetch*
  [context-name external-ids boundaries]
  (let [targets (vals external-ids)
        boundaries (distinct (map #(str "'" (name %) "'") (filter #(not (contains? external-ids %)) boundaries)))
        context-name (name context-name)
        q
        (if (empty? boundaries)
          (str "SELECT FROM (TRAVERSE V.in,E.out,V.out,E.in FROM [" (string/join ", " targets) "] WHERE (@class = 'OGraphVertex' OR context = '" context-name "')) WHERE @class = 'OGraphVertex'")
          (str "SELECT FROM (TRAVERSE V.out, E.in FROM (TRAVERSE V.in,E.out,V.out,E.in FROM [" (string/join ", " targets) "] WHERE ((@class = 'OGraphVertex' AND NOT (_hirop_type IN [" (string/join ", " boundaries) "])) OR context = '" context-name "')) WHERE ($depth < 3 AND (@class = 'OGraphVertex' OR context = '" context-name "'))) WHERE @class = 'OGraphVertex'"))]
    (with-db
      (doall
       (map #(orient->hirop % context-name)
            (oquery/sql-query q))))))


(defmethod fetch :orientdb
  [backend context]
  (fetch* (:name context) (:external-ids context) (hirop/get-free-external-doctypes context)))

(defmethod save :orientdb
  [backend store context]
  (let [sdocs (vals (:starred store))
        context-name (:name context)]
    (save* sdocs context-name)))

(defmethod history :orientdb
  [backend id]
  (with-db
    (doall
     (map hist->hirop
          (oquery/sql-query (str "SELECT FROM History WHERE _hirop_id = " id))))))

