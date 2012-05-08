(ns npmap.core
  (require [zutil.util :as z]
           [clojure.core.reducers :as r]
           [useful.parallel :as u])
  (:gen-class))

(defmacro BLOCK_LOW   [i p n] `(int (/ (* ~i ~n) ~p)))
(defmacro BLOCK_HIGH  [i p n] `(- (BLOCK_LOW (+ 1 ~i) ~p ~n) 1))
(defmacro BLOCK_SIZE  [i p n] `(- (BLOCK_LOW (+ 1 ~i) ~p ~n) (BLOCK_LOW ~i ~p ~n)))
(defmacro BLOCK_ONWER [j p n] `(int (/ (- (* (inc ~j) ~p) 1) ~n)))

(defn partition-work
  [p coll]
  (let [s (seq coll)
        n (count s)]
    (loop [w [] s s i 0]
      (if (< i p)
        (let [block-size (BLOCK_SIZE i p n)]
          (recur (conj w (take block-size s)) (drop block-size s) (inc i)))
        (seq w)))))

(defn npmap [p f coll]
  (apply concat
    (pmap #(doall (map f %))
      (partition-work p coll))))

(defn spin-mult
  [n] 
  (let [n (* n 1000)] 
    (loop [i 0] 
      (when (< i n) 
        (* 2 2) 
        (recur (inc i)))))) 

(defn fac 
  ([n] (fac n 1N))
  ([n x] 
     (if (= n 1)
       x
       (recur (- n 1) (* n x)))))

(def tests
     {"16/50000" (take 16 (repeat 50000))
      "1000/500" (take 1000 (repeat 500))
      "10000/50" (take 10000 (repeat 50))})

(defn -main []
  (doseq [the-test tests]
    (let [test-name (key the-test)
          test-data (val the-test)]
      (println test-name)
      (println "-- Factorial --")
      (println "Warming up...")
      (do (doall (pmap fac test-data)) nil)
      (println "Timing...")
      (println "map: ")
      (time (do (doall (map fac test-data)) nil))
      (println "pmap: ")
      (time (do (doall (pmap fac test-data)) nil))
      (println "npmap...")
      (dotimes [i (.availableProcessors (Runtime/getRuntime))]
        (println (+ 1 i) "cores: ")
        (time (do (doall (npmap (+ 1 i) fac test-data)) nil)))
      (println "zpmap...")
      (time (do (doall (z/zpmap fac test-data))
                nil))
      (println "pcollect...")
      (time (do (doall (u/pcollect fac test-data))
                nil))
      (println "reducers...")
      (time (do (into [] (r/map fac test-data))
                nil))

      (println "-- Multiplication --")
      (println "Warming up...")
      (do (doall (pmap spin-mult test-data)) nil)
      (println "Timing...")
      (println "map: ")
      (time (do (doall (map spin-mult test-data)) nil))
      (println "pmap: ")
      (time (do (doall (pmap spin-mult test-data)) nil))
      (println "npmap...")
      (dotimes [i (.availableProcessors (Runtime/getRuntime))]
        (println (+ 1 i) "cores: ")
        (time (do (doall (npmap (+ 1 i) spin-mult test-data)) nil)))
      (println "zpmap...")
      (time (do (doall (z/zpmap spin-mult test-data))
                nil))
      (println "pcollect...")
      (time (do (doall (u/pcollect spin-mult test-data))
                nil))
      (println "reducers...")
      (time (do (into [] (r/map spin-mult test-data))
                nil)))))