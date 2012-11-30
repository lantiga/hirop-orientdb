(ns hirop-orientdb.core-test
  (:use clojure.test
        hirop.backend
        hirop-orientdb.core)
  (:require [clj-orient.core :as ocore]))

(defn init-fresh [connection-data]
  (let [admin-connection-data
        {:connection-string "local:testdb"
         :username "admin"
         :password "admin"}]
    (alter-var-root #'*connection-data* (fn [_] admin-connection-data))
    (try
      (with-db
        (ocore/delete-db!))
      (catch Exception e (prn e)))
    (alter-var-root #'*connection-data* (fn [_] connection-data))
    (com.orientechnologies.orient.core.config.OGlobalConfiguration/dumpConfiguration(System/out))
    (try
      (ocore/create-db! (:connection-string *connection-data*))
      (catch Exception e (prn e)))))

(deftest save-fetch-test
  (let [connection-data
        {:connection-string "local:testdb"
         :username "writer"
         :password "writer"}
        sdocs
        [{:_id "tmp0" :_type "Foo" :title "First"}
         {:_id "tmp1" :_type "Bar" :Foo_ "tmp0" :title "Second"}
         {:_id "tmp2" :_type "Bar" :Foo_ "tmp0" :title "Third"}
         {:_id "tmp3" :_type "Baz" :Bar_ ["tmp1" "tmp2"] :title "Fourth"}]]
    (init-fresh connection-data)
    (let [res (save :orientdb sdocs :test)
          remap (:remap res)
          docs (fetch :orientdb :test {:Foo (remap "tmp0")} nil)]
      (is (= (set (:Bar_ (first (filter #(= (:_type %) "Baz") docs))))
             (set [(remap "tmp1") (remap "tmp2")]))))))

(deftest save-fetch-boundary-test
  (let [connection-data
        {:connection-string "local:testdb"
         :username "writer"
         :password "writer"}
        sdocs
        [{:_id "tmp0" :_type "Foo" :title "First1"}
         {:_id "tmp1" :_type "Bar" :Foo_ "tmp0" :Baz_ "tmp4" :title "Second1" :_meta {:tag "META"}}
         {:_id "tmp2" :_type "Foo" :title "First2"}
         {:_id "tmp3" :_type "Bar" :Foo_ "tmp2" :Baz_ "tmp4" :title "Second2"}
         {:_id "tmp4" :_type "Baz" :title "Third"}]]
    (init-fresh connection-data)
    (let [res (save :orientdb sdocs :test)
          remap (:remap res)
          docs (fetch :orientdb :test {:Foo (remap "tmp0")} nil)]
      (is (= (:_meta (first (filter :_meta docs)))
             {:tag "META"}))
      (is (= (set (map :title docs))
             #{"First1" "Second1" "Third" "Second2" "First2"})))
    (let [res (save :orientdb sdocs :test)
          remap (:remap res)
          docs (fetch :orientdb :test {:Foo (remap "tmp0")} [:Foo :Baz])]
      (is (= (set (map :title docs))
             #{"First1" "Second1" "Third"})))))
