;; -*- mode: lisp; indent-tabs-mode: nil; -*-

(defpackage :example
  (:use :cl
	:cl-user))

(in-package :example)

(defparameter *stomp* nil)
(defparameter *health-request* "/topic/health-request")
(defparameter *health-response* "/topic/health-response")
(defparameter *host* "localhost")
(defparameter *port* 61613)
(defparameter *counter* 0)

(defun out (fmt &rest arguments)
  (apply #'format *standard-output* fmt arguments)
  (finish-output *standard-output*)
  (force-output *standard-output*))

(defun callback (frame)
  (incf *counter*)
  (out "[~a]~%" (stomp:frame-body-of frame))
  (let ((msg (format nil "<pong><name>foo</name><count>~a</count></pong>" *counter*)))
    (stomp:post *stomp* msg *health-response*)))

(defun bark (frame)
  (let ((body (stomp:frame-body-of frame)))
    (out "--> bark : ~a~%" body)))

(defun chirp (frame)
  (let ((body (stomp:frame-body-of frame)))
    (out "--> chirp: ~a~%" body)))

(defun run ()
  (setf *stomp* (stomp:make-connection *host* *port*))
  (stomp:connect *stomp*)
  (stomp:register *stomp* #'callback *health-request*)
  (stomp:register *stomp* #'chirp *health-response*)
  (stomp:register *stomp* #'bark *health-response*)
  (stomp:start *stomp*))

(defun start ()
  (sb-thread:make-thread (lambda () (run)) :name "stomp-subscribe"))

(defun stop ()
  (cl-stomp:stop *stomp*))

