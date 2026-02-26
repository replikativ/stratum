(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'org.replikativ/stratum)
(def version (format "0.1.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def java-src-dir "src-java")

;; Basis with :release alias — used for jar/deploy (published deps)
(def release-basis (delay (b/create-basis {:project "deps.edn"
                                           :aliases [:release]})))
;; Basis without :release — used for compile-java (works with local deps)
(def dev-basis (delay (b/create-basis {:project "deps.edn"})))

(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def uber-file (format "target/%s-%s-standalone.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile-java
  "Compile Java SIMD sources to target/classes.
   Cleans stale .class files first, then compiles all Java sources.
   Usage: clj -T:build compile-java"
  [_]
  (b/delete {:path class-dir})
  (println "Compiling Java sources...")
  (b/javac {:src-dirs [java-src-dir]
            :class-dir class-dir
            :basis @dev-basis
            :javac-opts ["--release" "21"
                         "--add-modules" "jdk.incubator.vector"
                         "-Xlint:-preview"]})
  (println "Done."))

(defn prep [_]
  (compile-java nil))

(defn jar [_]
  (compile-java nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @release-basis
                :src-dirs ["src"]
                :scm {:url "https://github.com/replikativ/stratum"
                      :connection "scm:git:git://github.com/replikativ/stratum.git"
                      :developerConnection "scm:git:ssh://git@github.com/replikativ/stratum.git"
                      :tag (str "v" version)}
                :pom-data [[:description "SIMD-accelerated columnar analytics for the JVM"]
                           [:url "https://github.com/replikativ/stratum"]
                           [:licenses
                            [:license
                             [:name "Apache License 2.0"]
                             [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]]})
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file})
  (println (str "JAR: " jar-file " (version " version ")")))

(defn uber [_]
  (compile-java nil)
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/compile-clj {:basis @release-basis
                  :class-dir class-dir
                  :src-dirs ["src"]
                  :ns-compile '[stratum.server]
                  :java-opts ["--add-modules=jdk.incubator.vector"
                              "--enable-native-access=ALL-UNNAMED"]})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis @release-basis
           :main 'stratum.server})
  (println (str "Uberjar: " uber-file " (version " version ")")))

(defn install [_]
  (jar nil)
  (b/install {:basis @release-basis
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir})
  (println (str "Installed " lib " " version " to local Maven repo")))

(defn deploy [_]
  (jar nil)
  (let [dd (requiring-resolve 'deps-deploy.deps-deploy/deploy)]
    (dd {:installer :remote
         :artifact (b/resolve-path jar-file)
         :pom-file (b/pom-path {:lib lib :class-dir class-dir})})))
