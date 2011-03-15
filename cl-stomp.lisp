;; -*- mode: lisp; indent-tabs-mode: nil; -*-
;;
;; Available under MIT-style License. see COPYING.
;;

(defpackage :cl-stomp

  (:nicknames :stomp)

  (:use :cl
        :cl-user)

  (:export :make-connection
           :start
           :stop
           :frame
           :frame-body-of
           :frame-name-of
           :frame-headers-of
           :set-header
           :get-header
           :set-destination
           :get-destination
           :register
           :register-error
           :subscribe
           :unsubscribe
           :post
           :ack
           :begin
           :commit
           :rollback))

(in-package :cl-stomp)

;;-------------------------------------------------------------------------
;; convenience utils

#+nil
(defun log-debug (fmt &rest args)
  (apply #'format *standard-output* fmt args)
  (finish-output *standard-output*))

#-nil
(defun log-debug (fmt &rest args)
  (declare (ignore fmt args)))

(defun string-from-bytes (bytes)
  (map 'string #'code-char bytes))

(defun string-to-bytes (string)
  (map 'sequence 'char-code string))

(defun string-strip (str)
  "Remove spaces, tabs and line enders from a string."
  (declare (string str))
  (string-trim (list #\Space #\NewLine #\Return #\Tab #\Nul) str))

(defun string-join (lines)
  (let ((result (make-array '(0) :element-type 'character :fill-pointer 0 :adjustable t)))
    (with-output-to-string (stream result)
      (dolist (line lines)
        (write-line line stream)))))

(defun string-split (string delim)
  "Splits STRING at the first occurance of DELIM and returns the before and after subsequences.
   If DELIM is not found in STRING, returns STRING and NIL."
  (when string
    (let ((start (search delim string :test 'string=)))
      (if (null start)
        (values string nil)
        (let ((start2 (min (length string) (+ (length delim) start))))
          (values (subseq string 0 start)
                  (subseq string start2)))))))

(defun string-lines (string)
  (let ((result ()))
    (with-input-from-string (stream string)
      (loop 
         :for line = (read-line stream nil 'eof)
         :while (not (eql line 'eof))
         :do (setf result (pushnew line result))))
    (nreverse result)))

;;-------------------------------------------------------------------------
;; frame

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
  "Construct a frame by parsing STRING according to protocol."
  ;; declare some useful local functions
  (labels (
           (get-line (stream)
             (let ((line (read-line stream nil 'eof)))
               (unless (eql line 'eof)
                 line)))

           (make-header (line)
             (multiple-value-bind (before after) 
                 (string-split line ":")
               (list (string-strip before) (string-strip after))))

           ;; frame name is first line
           (get-name (stream)
             (get-line stream))

           ;; frame headers are second lines through to empty line
           (get-headers (stream)
             (loop
               :for line = (get-line stream)
               :while (> (length line) 0)
               :collect (make-header line)))

           ;; frame body is all lines after the empty line
           (get-body (stream)
             (coerce (loop
                       :for c = (read-char stream nil 'eof)
                       :while (not (eql c 'eof))
                       :collect c)
               'string)))

    (with-input-from-string (stream string)
      (let ((name (get-name stream))
            (headers (get-headers stream))
            (body (get-body stream)))
        (make-instance 'frame :name name :headers headers :body body)))))

(defmethod print-object ((self frame) stream)
  "Print FRAME to STREAM according to protocol."
  (with-slots (name headers body) self
    (format stream "~A~%" name)
    (dolist (header headers)
      (format stream "~a:~a~%" (first header) (second header)))
    (format stream "~%~a~%~a~%" body (code-char 0))))

(defmethod set-destination ((self frame) destination)
  "Set the destination header for FRAME."
  (set-header self "destination" destination))

(defmethod set-client-ack ((self frame))
  "Specify a 'client' ack header for FRAME"
  (set-header self "ack" "client"))

(defun header= (header1 header2)
  "Case insensitive comparison func for headers."
  (string= (string-downcase header1) (string-downcase header2)))

(defmethod get-header ((self frame) key)
  "Return the value of the header named KEY, or NIL if it doesn't exist."
  (with-slots (headers) self
    (second (assoc key headers :test #'header=))))

(defmethod get-destination ((self frame))
  "Return the value of the destination header."
  (get-header self "destination"))

(defmethod set-header ((self frame) key value)
  "Add a header named KEY to FRAME with VALUE.
   If the header already exists, VALUE is appended to the existing value(s)."
  (when value
    (with-slots (headers) self
      (if (not (assoc key headers :test #'header=))
        (setf headers (append (list (list key value)) headers))
        (let ((result))
          (dolist (header headers)
            (if (header= (first header) key)
              (push (list key value) result)
              (push header result)))
          (setf headers result))))))

(defmethod error-frame-p ((self frame))
  (string= (string-downcase (frame-name-of self)) "error"))

;;-------------------------------------------------------------------------
;; stomp

(defgeneric get-header (frame key)
  (:documentation ""))

(defgeneric set-header (frame key value)
  (:documentation ""))

(defgeneric get-destination (frame)
  (:documentation ""))

(defgeneric set-destination (frame destination)
  (:documentation "Set the destination header for the frame."))

(defgeneric post (connection message destination &optional headers)
  (:documentation "Post a message to a destination."))

(defgeneric ack (connection frame &optional transaction)
  (:documentation "Send the client ack for frame."))

(defgeneric begin (connection transaction)
  (:documentation "Start a transaction for the given name"))

(defgeneric commit (connection transaction)
  (:documentation "Commit a transaction for the given name"))

(defgeneric rollback (connection transaction)
  (:documentation "Abort a transaction for the given name"))

(defgeneric register (connection callback destination &optional client-ack?)
  (:documentation "Register a listener for messages to a destination.
                   Callback should be a lambda which accepts a frame argument.
                   If client-ack? is t, client is responsible for sending ACK."))

(defgeneric register-error (connection callback)
  (:documentation "Register a listener for STOMP error frames."))

(defgeneric subscribe (connection destination &optional client-ack?)
  (:documentation "Subscribe to a topic or queue."))

(defgeneric unsubscribe (connection destination)
  (:documentation "Unsubscribe from a topic or queue."))

(defgeneric start (connection &key username passcode)
  (:documentation "Start listening for messages from stomp."))

(defgeneric stop (connection)
  (:documentation "Stop the connection with stomp."))

(defclass registration ()
  ((callback    :initarg :callback)       ; The callback func
   (destination :initarg :destination)    ; The topic/queue name
   (client-ack? :initarg :client-ack?)))  ; Use client (or auto) ack?

(defclass connection ()
  ((host
    :initform "localhost"
    :initarg :host)
   (port
    :initform 61613
    :initarg :port)
   (stream
    :initform nil
    :initarg :stream)
   (registrations
    :initform '()
    :initarg :registrations)
   (error-callback
     :initform nil)
   (terminate
    :initform nil)))

(defun make-connection (host port)
  (make-instance 'connection :host host :port port))

(defmethod start ((self connection) &key username passcode)
  "Connects to the message broker, sends subscriptions for any existing registrations,
   and enters a receive loop."
  (handler-bind
      ((t (lambda (e)
            (disconnect self)
            (log-debug "error: ~a" e))))
    (with-slots (host port stream registrations terminate) self
      (usocket:with-client-socket (socket strm host port
                                    :protocol :stream
                                    :element-type '(unsigned-byte 8))
        (setf stream strm)
        ;;send CONNECT frame
        (let ((frame (make-instance 'frame :name "CONNECT")))
          (set-header frame "login" username)
          (set-header frame "passcode" passcode)
          (send self frame))
        
        ;;send SUBSCRIBE frames
        (loop 
          :for reg :in registrations
          :do (with-slots (destination client-ack?) reg
                (subscribe self destination client-ack?)))

        ;;receive loop
        (setf terminate nil)
        (let ((recvbuf ""))
          (loop
            :until terminate
            :do (let ((sock (car (usocket:wait-for-input socket :timeout 0))))
                  (if sock
                    (let* ((recvstr (receive self))
                           (newbuf (if (= (length recvbuf) 0)  ;; only call string-join when necessary
                                     recvstr
                                     (string-join (list recvbuf recvstr)))))
                      (setf recvbuf (process_receive_buffer self newbuf)))
                    (sleep 0.1)))))
        (log-debug "terminated")))))
        
(defmethod stop ((self connection))
  "Gracefully terminates the current receive loop and closes the connection to the message broker."
  (with-slots (terminate) self
    (setf terminate t)))

(defmethod apply-callbacks ((self connection) (frame frame))
  "Send FRAME to any matching registered callbacks."
  (with-slots (registrations error-callback) self
    (if (error-frame-p frame)
      (when error-callback
        (funcall error-callback frame))
      (let ((dest (get-destination frame)))
        (loop 
          :for reg :in registrations
          :do (with-slots (callback destination) reg
                (if (string= dest destination)
                  (funcall callback frame))))))))

(defmethod send ((self connection) (frame frame))
  (send self (with-output-to-string (stream)
               (print frame stream))))

(defmethod send ((self connection) string)
  (with-slots (stream) self
    (write-sequence (string-to-bytes string) stream)
    (finish-output stream)))
  
(defmethod receive ((self connection))
  "Called whenever there's activity on the connection stream.
   Reads from the stream and returns the received buffer as a string."
  (with-slots (stream) self
      (let ((buffer (loop
                      :for b = (read-byte stream nil 'eof)
                      :while (listen stream)
                      :collect b)))

        (if (> (length buffer) 0)
          
          ;; return the buffer as a string
          (string-from-bytes buffer)

          ;; otherwise, it means the other end has terminated,
          ;; so close things down
          (progn
            (log-debug "nothing to read from socket, closing.~%")
            (disconnect self))))))

(defmethod process_receive_buffer ((self connection) buffer)
  "Try to extract and process frame(s) from recvbuf.  Returns unprocessed buffer."
  (labels ((process-frame (frame)
             (log-debug "frame: ~a" frame)
             (apply-callbacks self frame))

            (extract-frame ()
              (multiple-value-bind (before after)
                  (string-split buffer (string (code-char 0)))
                (when after
                  ;;got one
                  (process-frame (make-frame before))
                  (setf buffer after)))))

    (loop 
      :while (extract-frame))

    buffer))

(defmethod subscribe ((self connection) destination &optional client-ack?)
  (let ((frame (make-instance 'frame :name "SUBSCRIBE")))
    (set-destination frame destination)
    (when client-ack?
      (set-client-ack frame))
    (send self frame)))

(defmethod unsubscribe ((self connection) destination)
  (let ((frame (make-instance 'frame :name "UNSUBSCRIBE")))
    (set-destination frame destination)
    (send self frame)))

(defmethod register ((self connection) callback destination &optional client-ack?)
  "Register a callback func for a destination.  A subscription to the destination using
   the optional client-ack? is issued for all callbacks as part of connecting to the mq server."
  (with-slots (stream registrations) self
    (if stream
      (subscribe self destination client-ack?))
    (setf registrations (append registrations (list (make-instance 'registration 
                                                      :callback callback
                                                      :destination destination
                                                      :client-ack? client-ack?))))))

(defmethod register-error ((self connection) callback)
  "Register a callback func for STOMP error frames."
  (with-slots (error-callback) self
    (setf error-callback callback)))

(defmethod post ((self connection) message destination &optional headers)
  (let ((frame (make-instance 'frame :name "SEND" :body (string-strip message))))
    (set-destination frame destination)
    (loop 
      :for (key value) :in headers
      :do (set-header frame key value))
    (send self frame)))

(defmethod ack ((self connection) frame &optional transaction)
  "Send the client ack for FRAME and optional TRANSACTION"
  (let ((ack-frame (make-instance 'frame :name "ACK")))
    (set-header ack-frame "message-id" (get-header frame "message-id"))
    (set-header ack-frame "transaction" transaction)
    (send self ack-frame)))

(defmethod begin ((self connection) transaction)
  "Begin a transaction with name TRANSACTION"
  (let ((frame (make-instance 'frame :name "BEGIN")))
    (set-header frame "transaction" transaction)
    (send self frame)))

(defmethod commit ((self connection) transaction)
  "Commit a transaction with name TRANSACTION"
  (let ((frame (make-instance 'frame :name "COMMIT")))
    (set-header frame "transaction" transaction)
    (send self frame)))

;; naming this method 'abort' is not a good idea, so naming it 'rollback' instead
(defmethod rollback ((self connection) transaction)
  "Abort a transaction with name TRANSACTION"
  (let ((frame (make-instance 'frame :name "ABORT")))
    (set-header frame "transaction" transaction)
    (send self frame)))

(defmethod disconnect ((self connection))
  (with-slots (stream) self
    (when (and stream (open-stream-p stream))
      (let ((frame (make-instance 'frame :name "DISCONNECT")))
        (send self frame))
      (close stream)
      (setf stream nil))))
