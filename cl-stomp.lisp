;; -*- mode: lisp; -*-

(defpackage :cl-stomp

  (:nicknames :stomp)

  (:use :cl
        :cl-user)

  (:export :frame
           :set-header
           :get-header
           :set-destination
           :get-destination
           :frame-body-of
           :frame-name-of
           :stomp-connection
           :make-connection
           :connect
           :register
           :post
           :start
           :stop))

(in-package :cl-stomp)

;;-------------------------------------------------------------------------
;; socket interface

(defun ip-addr-of (host-name)
  (let ((host (sb-bsd-sockets:get-host-by-name host-name)))
    (sb-bsd-sockets:host-ent-address host)))

(defun inet-addr (ip-address)
  (sb-bsd-sockets:make-inet-address ip-address))

(defun sk-connect (sock address port)
  (let ((ip-address (if (stringp address) (inet-addr address) address)))
    (sb-bsd-sockets:socket-connect sock ip-address port)))

(defun sk-file-desc (sock)
  (sb-bsd-sockets:socket-file-descriptor sock))

(defun sk-close (sock)
  (let ((fd (sb-bsd-sockets:socket-file-descriptor sock)))
    (sb-sys:invalidate-descriptor fd)
    (sb-bsd-sockets:socket-close sock)))

(defun sk-make ()
  (let ((sock (make-instance 'sb-bsd-sockets:inet-socket 
                             :type :stream :protocol :tcp)))
    (setf (sb-bsd-sockets:sockopt-reuse-address sock) t)
    sock))

;;-------------------------------------------------------------------------
;; convenience utils

(defun log-debug (fmt &rest args)
  (apply #'format *standard-output* fmt args)
  (finish-output *standard-output*))

(defun string-from-bytes (bytes)
  (map 'string #'code-char bytes))

(defun string-strip (str)
  "Remove spaces, tabs and line enders from a string."
  (declare (string str))
  (string-trim (list #\Space #\NewLine #\Return #\Tab #\Nul) str))

(defun string-join (lines)
  (let ((result (make-array '(0) :element-type 'character :fill-pointer 0 :adjustable t)))
    (with-output-to-string (stream result)
      (dolist (line lines)
        (write-line line stream)))
    (string-strip result)))

(defun string-split (str &key (delim " ") (limit nil))
  "Returns a list of words in STR broken at the DELIM boundary."

  (labels ((pop-word (str &key (delim " "))
             "Returns the first word and rest of STR broken at DELIM."
             (let ((start (position delim str :test 'string=)))
               (if (null start)
                   (values str nil)
                   (let ((start2 (min (length str)
                                      (+ 1 start))))
                     (values (subseq str 0 start)
                             (subseq str start2))))))

           (splitter (str delim limit count)
             (if (null str)
                 nil
                 (if (and (not (null limit))
                          (>= count limit))
                     (list str)
                     (multiple-value-bind (word rest) (pop-word str :delim delim)
                       (if (not (zerop (length word)))
                           (append (list (string-strip word)) 
                                   (splitter rest delim limit (incf count)))
                           (splitter rest delim limit (incf count))))))))

    (splitter str delim limit 0)))

(defun string-lines (string)
  (let ((result ()))
    (with-input-from-string (stream string)
      (loop 
         :for line = (read-line stream nil 'eof)
         :while (not (eql line 'eof))
         :do (setf result (pushnew line result))))
    (nreverse result)))

(defun string-contains (string str-or-char-list)
  "Returns true if STR-OR-CHAR-LIST is contained in STRING."
  (declare (type string string))
  (search (coerce str-or-char-list 'string) string :test #'string=))

;;-------------------------------------------------------------------------
;; frame

(defgeneric set-destination (frame destination)
  (:documentation "Set the destination header for the frame."))

(defgeneric get-header (frame key)
  (:documentation ""))

(defgeneric get-destination (frame)
  (:documentation ""))

(defgeneric set-header (frame key value)
  (:documentation ""))

(defclass frame ()
  ((name
    :initform "MESSAGE"
    :initarg :name
    :accessor frame-name-of)
   (headers
    :initform ()
    :initarg :headers
    :accessor frame-headers-of)
   (body
    :initform ""
    :initarg :body
    :accessor frame-body-of)))

(defun make-frame (string)

  ;; declare some useful local functions
  (labels (

           (make-header (line)
             (map 'list #'string-strip (string-split line :delim ":" :limit 1)))

           ;; frame name is first line
           (find-name (source)
             (first source))

           ;; frame headers are second lines through to empty line
           (find-headers (source)
             (let ((lines (rest source)))
               (loop
                  :for line :in lines
                  :while (> (length line) 0)
                  :collect (make-header line))))

           ;; frame body is all lines after the empty line
           (find-body (source)
             (let* ((lines (reverse source))
                    (result (loop
                               :for line :in lines
                               :while (> (length line) 0)
                               :collect line)))
               (string-join (nreverse result)))))

    (let ((lines (string-lines string)))
      (let ((name (find-name lines))
            (headers (find-headers lines))
            (body (find-body lines)))
        (make-instance 'frame :name name :headers headers :body body)))))

(defmethod print-object ((self frame) stream)
  (with-slots (name headers body) self
    (format stream "~A~%" name)
    (dolist (header headers)
      (format stream "~a:~a~%" (first header) (second header)))
    (format stream "~%~a~%~a~%" body (code-char 0))))

(defmethod set-destination ((self frame) destination)
  (set-header self "destination" destination))

(defmethod get-header ((self frame) key)
  (with-slots (headers) self
    (second (assoc key headers :test #'string=))))

(defmethod get-destination ((self frame))
  (get-header self "destination"))

(defmethod set-header ((self frame) key value)
  (with-slots (headers) self
    (if (not (assoc key headers :test #'string=))
        (setf headers (append (list (list key value)) headers))
        (let ((result))
          (dolist (header headers)
            (if (string= (first header) key)
                (push (list key value) result)
                (push header result)))
          (format t "result: ~s~%" result)
          (setf headers result)))))

;;-------------------------------------------------------------------------
;; stomp

(defgeneric connect (connection &key user pass)
  (:documentation "Connect to the stomp server."))

(defgeneric disconnect (connection)
  (:documentation "Disconnection from the stomp server."))

(defgeneric send (connection frame)
  (:documentation "Send a string to stomp."))

(defgeneric post (connection message destination)
  (:documentation "Post a message to a destination."))

(defgeneric register (connection callback destination)
  (:documentation "Register a listener for messages to a destination."))

(defgeneric receive (connection)
  (:documentation "Read data from stomp."))

(defgeneric subscribe (connection destination)
  (:documentation "Subscribe to a topic."))

(defgeneric start (connection)
  (:documentation "Start listening for messages from stomp."))

(defgeneric apply-callbacks (instance frame)
  (:documentation "Send message to registered callbacks."))

(defgeneric stop (connection)
  (:documentation "Stop the connection with stomp."))

(defclass stomp-connection ()
  ((host
    :initform "localhost"
    :initarg :host
    :accessor host-of)
   (port
    :initform 61613
    :initarg :port
    :accessor port-of)
   (ip
    :initform "127.0.0.1"
    :initarg :ip
    :accessor ip-of)
   (socket
    :initform nil
    :initarg :socket
    :accessor socket-of)
   (fd
    :initform nil
    :initarg :fd
    :accessor fd-of)
   (handler
    :initform nil
    :initarg :handler
    :accessor handler-of)
   (callbacks
    :initform ()
    :initarg :callbacks
    :accessor callbacks-of)
   (terminate
    :initform nil)))

(defun make-connection (host port)
  (make-instance 'stomp-connection :host host :port port))

(defmethod connect ((self stomp-connection) &key user pass)
  (handler-case 

      ;; open the socket and send the connect string

      (with-slots (host port ip socket fd handler) self
        (setf ip (ip-addr-of host))
        (setf socket (sk-make))
        (sk-connect socket ip port)
        (setf fd (sk-file-desc socket))
        (setf handler (sb-sys:add-fd-handler fd :input (lambda (x)
                                                         (declare (ignore x))
                                                         (receive self))))
        (let ((frame (make-instance 'frame :name "CONNECT")))
          (if user
              (set-header frame "login" user))
          (if pass
              (set-header frame "passcode" pass))
          (send self frame)))

    ;; just log the error for now

    (condition (c)
      (setf (socket-of self) nil)
      (log-debug "error-on-connect: ~a" c))))

(defmethod subscribe ((self stomp-connection) destination)
  (let ((frame (make-instance 'frame :name "SUBSCRIBE")))
    (set-destination frame destination)
    (send self frame)))

(defmethod start ((self stomp-connection))
  (with-slots (terminate) self
    (setf terminate nil)
    (loop
       :until terminate
       :do (sb-sys:serve-all-events 1))
    (log-debug "terminated")))
    
(defmethod stop ((self stomp-connection))
  (with-slots (terminate) self
    (setf terminate t)))

(defmethod send ((self stomp-connection) (frame frame))
  (with-slots (fd) self
    (let ((stream (sb-sys:make-fd-stream fd :output t :element-type 'character :buffering :none)))
      (print frame stream)
      (finish-output stream))))

(defmethod send ((self stomp-connection) string)
  (with-slots (fd) self
    (let ((stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8)
                                         :buffering :none)))
      (write-sequence (sb-ext:string-to-octets string) stream)
      (finish-output stream))))

(defmethod apply-callbacks ((self stomp-connection) (frame frame))
  (with-slots (callbacks) self
    (let ((destination (get-destination frame)))
      (loop 
         :for (dest func) :in callbacks
         :do (if (string= dest destination)
                 (funcall func frame))))))
  
(defmethod receive ((self stomp-connection))
  "Called whenever there's activity on the file descriptor."
  (with-slots (socket fd handler callbacks) self
    (let ((str (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8)
                                      :buffering :full)))
      (let ((buffer (loop 
                       :for b = (read-byte str nil 'eof)
                       :while (listen str)
                       :collect b)))

        (if (> (length buffer) 0)

            ;; if we got something, send it to all the
            ;; registered callbacks

            (let ((frame (make-frame (string-from-bytes buffer))))
              (apply-callbacks self frame))

            ;; otherwise, it means the other end has terminated,
            ;; so close things down (eventually go into a sleep/wait
            ;; loop in case of reconnection

            (handler-case
                (progn
                  (log-debug "nothing to read from socket~%")
                  (disconnect self)
                  (sb-sys:remove-fd-handler handler)
                  (sk-close socket))
              (condition (c)
                (log-debug "close: ~a" c))))))))

(defmethod register ((self stomp-connection) callback destination)
  (with-slots (callbacks) self
    (subscribe self destination)
    (setf callbacks (append callbacks (list (list destination callback))))))

(defmethod post ((self stomp-connection) message destination)
  (let ((frame (make-instance 'frame :name "SEND" :body message)))
    (set-destination frame destination)
    (send self frame)))

(defmethod disconnect ((self stomp-connection))
  (let ((frame (make-instance 'frame :name "DISCONNECT")))
    (send self frame))
  (with-slots (handler socket) self
    (handler-case
        (sb-sys:remove-fd-handler handler)
      (condition (c)
        (log-debug "discconnect-handler-error: ~a" c)))
    (handler-case
        (sk-close socket)
      (condition (c)
        (log-debug "disconnect-socket-error: ~a" c)))))

