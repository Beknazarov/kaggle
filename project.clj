(defproject kaggle "0.0.1"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/tools.nrepl "0.2.0-RC1"]
                 [org.clojure/tools.macro "0.1.1"]
                 [pony "0.1.0-SNAPSHOT"]]
  :repositories {"releases" ~(str (.toURI
                                   (java.io.File. "~/.m2/repository")))})
