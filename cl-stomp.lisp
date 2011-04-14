;;; -*- mode: lisp; indent-tabs-mode: nil; -*-
;;;
;;; Available under MIT-style License. see COPYING.
;;;

(defpackage :cl-stomp

  (:nicknames :stomp)

  (:use :cl
        :cl-user)

  (:export :make-connection
           :start
           :stop
           :frame
           :frame-body
           :frame-name
           :frame-headers
           :get-header
           :set-header
           :get-destination
           :set-destination
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


;;;-------------------------------------------------------------------------
;;; Convenience utils

#+nil
(defun log-debug (fmt &rest args)
  (fresh-line *standard-output*)
  (apply #'format *standard-output* fmt args)
  (fresh-line *standard-output*)
  (force-output *standard-output*))

#-nil
(defun log-debug (fmt &rest args)
  (declare (ignore fmt args)))

(defun string-strip (string)
  "Remove spaces, tabs and line enders from a string."
  (check-type string string)
  (string-trim '(#\Space #\NewLine #\Return #\Tab #\Nul) string))

(defun string-split (string delim)
  "Splits STRING at the first occurrence of DELIM and returns the substrings before and after it.
   If DELIM is not found in STRING, returns STRING and NIL."
  (when string
    (let ((start (search delim string :test 'string=)))
      (if (null start)
        (values string nil)
        (let ((start2 (min (length string) (+ (length delim) start))))
          (values (subseq string 0 start)
                  (subseq string start2)))))))


;;;-------------------------------------------------------------------------
;;; The CL-STOMP API

(defgeneric get-header (frame key)
  (:documentation "Return the value of the header named KEY, or NIL if it doesn't exist."))

(defgeneric set-header (frame key value)
  (:documentation "Add a header named KEY to FRAME with VALUE.
                   If the header already exists, VALUE replaces the existing value."))

(defgeneric get-destination (frame)
  (:documentation "Return the destination header for FRAME."))

(defgeneric set-destination (frame destination)
  (:documentation "Set the destination header for FRAME."))

(defgeneric start (connection &key username passcode)
  (:documentation "Start listening for messages from STOMP."))

(defgeneric stop (connection)
  (:documentation "Stop the connection with STOMP."))

(defgeneric register (connection callback destination &optional client-ack?)
  (:documentation "Register a listener for messages to a destination.
                   CALLBACK should be a function which accepts a frame argument.
                   If CLIENT-ACK? is T, the client is responsible for sending ACK."))

(defgeneric register-error (connection callback)
  (:documentation "Register a listener for STOMP error frames."))

(defgeneric subscribe (connection destination &optional client-ack?)
  (:documentation "Subscribe to a topic or queue."))

(defgeneric unsubscribe (connection destination)
  (:documentation "Unsubscribe from a topic or queue."))

(defgeneric post (connection message destination &optional headers)
  (:documentation "Post a message to a destination.
                   HEADERS is an alist of header name and value pairs."))

(defgeneric ack (connection frame &optional transaction)
  (:documentation "Send the client an ACK for frame."))

(defgeneric begin (connection transaction)
  (:documentation "Start a transaction with the given name."))

(defgeneric commit (connection transaction)
  (:documentation "Commit a transaction with the given name."))

(defgeneric rollback (connection transaction)
  (:documentation "Abort a transaction with the given name."))


;;;-------------------------------------------------------------------------
;;; Frames

(defclass frame ()
  ((name :type string
         :initform "MESSAGE"
         :initarg :name
         :accessor frame-name)
   (headers :type list
            :initform ()
            :initarg :headers
            :accessor frame-headers)
   (body :type string
         :initform ""
         :initarg :body
         :accessor frame-body)))

(defun make-frame-from-string (string)
  "Construct a frame by parsing STRING according to the STOMP protocol."
  ;; Declare some useful local functions
  (labels ((get-line (stream)
             (let ((line (read-line stream nil 'eof)))
               (unless (eql line 'eof)
                 line)))
           (make-header (line)
             (multiple-value-bind (before after) 
                 (string-split line ":")
               (list (string-strip before) (string-strip after))))
           ;; Frame name is first line
           (get-name (stream)
             (get-line stream))
           ;; Frame headers are second lines through to empty line
           (get-headers (stream)
             (loop for line = (get-line stream)
                   while (> (length line) 0)
                   collect (make-header line)))
           ;; Frame body is all the lines after the empty line
           (get-body (stream)
             (coerce (loop for c = (read-char stream nil 'eof)
                           while (not (eql c 'eof))
                           collect c)
                     'string)))
    (with-input-from-string (stream string)
      (let ((name (get-name stream))
            (headers (get-headers stream))
            (body (get-body stream)))
        (make-instance 'frame
          :name name
          :headers headers
          :body body)))))

;; Makes a frame with the given name and headers,
;; evaluates the body,
;; and then sends the frame over the connection
(defmacro sending-frame ((connection vframe name &rest headers) &body body)
  `(let ((,vframe (make-instance 'frame :name ,name)))
     ,@(loop for (key val) on headers by #'cddr
             collect (list 'set-header vframe key val))
     (progn ,@body)
     (send ,connection ,vframe)))

;; Note that this is not a pretty-printing function
;; The format of the output is what's expected by the STOMP protocol
(defmethod print-object ((frame frame) stream)
  "Print FRAME to STREAM according to the STOMP protocol."
  (with-slots (name headers body) frame
    (format stream "~A~%" name)
    (dolist (header headers)
      (format stream "~A:~A~%" (first header) (second header)))
    (format stream "~%~A~%~A~%" body (code-char 0))))

(defun header= (header1 header2)
  "Case insensitive comparison function for headers."
  (string-equal (string header1) (string header2)))

(defmethod get-header ((frame frame) (key string))
  "Return the value of the header named KEY, or NIL if it doesn't exist."
  (with-slots (headers) frame
    (second (assoc key headers :test #'header=))))

(defmethod set-header ((frame frame) (key string) value)
  "Add a header named KEY to FRAME with VALUE, which can be of any type.
   If the header already exists, VALUE replaces the existing value."
  (when value
    (with-slots (headers) frame
      (if (not (assoc key headers :test #'header=))
        (setf headers (append (list (list key value)) headers))
        (let ((result ()))
          (dolist (header headers)
            (if (header= (first header) key)
              (push (list key value) result)
              (push header result)))
          (setf headers result))))))

(defmethod get-destination ((frame frame))
  "Return the destination header for FRAME."
  (get-header frame "destination"))

(defmethod set-destination ((frame frame) (destination string))
  "Set the destination header for FRAME."
  (set-header frame "destination" destination))

(defmethod set-client-ack ((frame frame))
  "Specify a 'client' ack header for FRAME"
  (set-header frame "ack" "client"))

(defmethod error-frame-p ((frame frame))
  (string-equal (frame-name frame) "error"))


;;;-------------------------------------------------------------------------
;;; Registrations

(defclass registration ()
  ((callback    :type (or null function) ;the callback function
                :initform nil
                :initarg :callback)
   (destination :type string             ;the topic/queue name
                :initarg :destination) 
   (client-ack? :initarg :client-ack?))) ;use client (or auto) ack?


;;;-------------------------------------------------------------------------
;;; Connections

(defclass connection ()
  ((host :type string
         :initform "localhost"
         :initarg :host)
   (port :type integer
         :initform 61613
         :initarg :port)
   (stream :initform nil
           :initarg :stream)
   (registrations :type list
                  :initform ()
                  :initarg :registrations)
   (error-callback :type (or null function)
                   :initform nil)
   (terminate :initform nil)))

(defun make-connection (host port)
  (check-type host string)
  (check-type port integer)
  (make-instance 'connection
    :host host
    :port port))

;;;-------------------------------------------------------------------------
;;; Implementation of the API

(defmethod start ((conn connection) &key username passcode)
  "Connects to the message broker, sends subscriptions for any existing registrations,
   and enters a receive loop."
  (handler-bind
      ((t (lambda (e)
            (disconnect conn)
            (log-debug "Error: ~A" e))))
    (with-slots (host port stream registrations terminate) conn
      (usocket:with-client-socket (socket strm host port
                                   :protocol :stream
                                   :element-type '(unsigned-byte 8))
        (setf stream strm)
        ;; Send CONNECT frame
        (connect conn username passcode)
        ;; Send SUBSCRIBE frames
        (loop for reg in registrations
              do (with-slots (destination client-ack?) reg
                   (subscribe conn destination client-ack?)))
        ;; The receive loop
        (setf terminate nil)
        (let ((recvbuf '()))
          (loop until terminate
                do (let ((sock (car (usocket:wait-for-input socket :timeout 1))))
                     (when sock
                       (let ((newbuf (append recvbuf (receive conn))))
                         (setf recvbuf (process-receive-buffer conn newbuf)))))))
        (log-debug "Terminated")))))
        
(defmethod stop ((conn connection))
  "Gracefully terminates the current receive loop and closes the connection to the message broker."
  (with-slots (terminate) conn
    (setf terminate t)))

(defmethod connect ((conn connection) &optional username passcode)
  (check-type username (or null string))
  (check-type passcode (or null string))
  (sending-frame (conn frame "CONNECT"
                       "login" username
                       "passcode" passcode)
    ))

(defmethod disconnect ((conn connection))
  (with-slots (stream) conn
    (when (and stream (open-stream-p stream))
      (sending-frame (conn frame "DISCONNECT")
        )
      (close stream)
      (setf stream nil))))

(defmethod send ((conn connection) (frame frame))
  (send conn (with-output-to-string (stream)
               ;; We use 'print' because the 'print-object' method on frames
               ;; does its output using the STOMP protocol
               (print frame stream))))

(defmethod send ((conn connection) (string string))
  (with-slots (stream) conn 
    (write-sequence (babel:string-to-octets string :encoding :utf-8) stream)
    (finish-output stream)))

(defmethod receive ((conn connection))
  "Called whenever there's activity on the connection stream.
   Reads from the stream and returns the received buffer as a list of bytes."
  (with-slots (stream) conn
    (let ((buffer (loop for b = (read-byte stream nil 'eof)
                        while (listen stream)
                        collect b)))
      (if (> (length buffer) 0)
        ;; Return the buffer
        buffer
        ;; Otherwise, it means the other end has terminated,
        ;; so close things down
        (progn
          (log-debug "Nothing to read from socket, closing.")
          (disconnect conn))))))

(defmethod process-receive-buffer ((conn connection) buffer)
  "Try to extract and process frame(s) from buffer.  Returns unprocessed buffer."
  (labels ((process-frame (frame)
             (log-debug "Frame: ~A" frame)
             (apply-callbacks conn frame))
           (extract-frame ()
             ;;Identify frames by looking for NULLs
             ;;This is safe with utf-8 because a 0 will never appear within multibyte characters
             ;;TODO: Use content-length header when provided instead of relying on NULL delimiter
             (let ((pos (position 0 buffer)))
               (when pos
                 (let* ((framebytes (subseq buffer 0 pos))
                        (framevector (coerce framebytes '(vector (unsigned-byte 8))))
                        (framestring (babel:octets-to-string framevector :encoding :utf-8)))
                   (process-frame (make-frame-from-string (string-strip framestring)))
                   (setf buffer (subseq buffer (+ pos 1))))))))
    (loop while (extract-frame))
    buffer))

(defmethod apply-callbacks ((conn connection) (frame frame))
  "Send FRAME to any matching registered callbacks."
  (with-slots (registrations error-callback) conn
    (if (error-frame-p frame)
      (when error-callback
        (funcall error-callback frame))
      (let ((dest (get-destination frame)))
        (loop for reg in registrations
              do (with-slots (callback destination) reg
                   (when (and callback (string-equal dest destination))
                     (funcall callback frame))))))))

(defmethod register ((conn connection) callback (destination string) &optional client-ack?)
  "Register a callback for a destination.  A subscription to the destination using the
   optional client-ack? is issued for all callbacks as part of connecting to the MQ server."
  (check-type callback (or null function))
  (with-slots (stream registrations) conn
    (when stream
      (subscribe conn destination client-ack?))
    (setf registrations (append registrations (list (make-instance 'registration 
                                                      :callback callback
                                                      :destination destination
                                                      :client-ack? client-ack?))))))

(defmethod register-error ((conn connection) callback)
  "Register an error callback for STOMP error frames."
  (check-type callback (or null function))
  (with-slots (error-callback) conn
    (setf error-callback callback)))

(defmethod subscribe ((conn connection) (destination string) &optional client-ack?)
  (sending-frame (conn frame "SUBSCRIBE"
                       "destination" destination)
    (when client-ack?
      (set-client-ack frame))))

(defmethod unsubscribe ((conn connection) (destination string))
  (sending-frame (conn frame "UNSUBSCRIBE"
                       "destination" destination)
    ))

(defmethod post ((conn connection) (message string) (destination string) &optional headers)
  (sending-frame (conn frame "SEND"
                       "destination" destination)
    (loop for (key value) in headers
          do (set-header frame key value))
    (setf (frame-body frame) (string-strip message))))

(defmethod ack ((conn connection) (for-frame frame) &optional transaction)
  "Send the client ack for FRAME and optional TRANSACTION"
  (check-type transaction (or null string))
  (sending-frame (conn frame "ACK"
                       "message-id" (get-header for-frame "message-id")
                       "transaction" transaction)
    ))

(defmethod begin ((conn connection) (transaction string))
  "Begin a transaction with name TRANSACTION"
  (sending-frame (conn frame "BEGIN"
                       "transaction" transaction)
    ))

(defmethod commit ((conn connection) (transaction string))
  "Commit a transaction with name TRANSACTION"
  (sending-frame (conn frame "COMMIT"
                       "transaction" transaction)
    ))

;; Naming this method 'abort' is not a good idea, so calling it 'rollback' instead
(defmethod rollback ((conn connection) (transaction string))
  "Abort a transaction with name TRANSACTION."
  (sending-frame (conn frame "ABORT"
                       "transaction" transaction)
    ))
