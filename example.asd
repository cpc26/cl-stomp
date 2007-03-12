;; -*- mode: lisp; -*-

(defsystem :example
    :version "0.1.0"
    :depends-on (cl-ppcre cl-stomp)
    :components
    ((:file "example")))
