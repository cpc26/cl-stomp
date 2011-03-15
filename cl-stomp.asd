;; -*- mode: lisp; indent-tabs-mode: nil; -*-
;;
;; Available under MIT-style License. see COPYING.
;;

(in-package :asdf)

(defsystem cl-stomp
  :description "Implements the STOMP protocol for connecting to a message broker."
  :author ""
  :version ""
  :licence ""
  :depends-on (cl-ppcre usocket)
  :components ((:file "cl-stomp")))


