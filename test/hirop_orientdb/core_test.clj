(ns hirop-orientdb.core-test
  (:use clojure.test
        hirop.backend
        hirop-orientdb.core)
  (:require [hirop.core :as hirop]
            [clj-orient.core :as ocore]))

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
    ;;(com.orientechnologies.orient.core.config.OGlobalConfiguration/dumpConfiguration(System/out))
    (try
      (ocore/create-db! (:connection-string *connection-data*))
      (catch Exception e (prn e)))))

(deftest save-fetch-test
  (let [connection-data
        {:connection-string "local:testdb"
         :username "writer"
         :password "writer"}
        sdocs
        [{:_hirop {:id "tmp0" :type "Foo"}
          :title "First"}
         {:_hirop {:id "tmp1" :type "Bar" :rels {:Foo "tmp0"}}
          :title "Second"}
         {:_hirop {:id "tmp2" :type "Bar" :rels {:Foo "tmp0"}}
          :title "Third"}
         {:_hirop {:id "tmp3" :type "Baz" :rels {:Bar ["tmp1" "tmp2"]}}
          :title "Fourth"}]]
    (init-fresh connection-data)
    (let [res (save :orientdb sdocs :test)
          remap (:remap res)
          docs (fetch :orientdb :test {:Foo (remap "tmp0")} nil)]
      (is (= (set (hirop/hrel (first (filter #(= (hirop/htype %) :Baz) docs)) :Bar))
             (set [(remap "tmp1") (remap "tmp2")]))))))

(deftest save-fetch-boundary-test
  (let [connection-data
        {:connection-string "local:testdb"
         :username "writer"
         :password "writer"}
        sdocs
        [{:_hirop {:id "tmp0" :type "Foo"}
          :title "First1"}
         {:_hirop {:id "tmp1" :type "Bar" :rels {:Foo "tmp0" :Baz "tmp4"} :meta {:tag "META"}}
          :title "Second1"}
         {:_hirop {:id "tmp2" :type "Foo"}
          :title "First2"}
         {:_hirop {:id "tmp3" :type "Bar" :rels {:Foo "tmp2" :Baz "tmp4"}}
          :title "Second2"}
         {:_hirop {:id "tmp4" :type "Baz"}
          :title "Third"}]]
    (init-fresh connection-data)
    (let [res (save :orientdb sdocs :test)
          remap (:remap res)
          docs (fetch :orientdb :test {:Foo (remap "tmp0")} nil)]
      (is (= (hirop/hmeta (first (filter hirop/hmeta docs)))
             {:tag "META"}))
      (is (= (set (map :title docs))
             #{"First1" "Second1" "Third" "Second2" "First2"})))
    (let [res (save :orientdb sdocs :test)
          remap (:remap res)
          docs (fetch :orientdb :test {:Foo (remap "tmp0")} [:Foo :Baz])]
      (is (= (set (map :title docs))
             #{"First1" "Second1" "Third"})))))
