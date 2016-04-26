package io.swiftworks.vertx.firebase.queue;

import com.englishtown.promises.Deferred;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.firebase.client.ChildEventListener;
import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.GenericTypeIndicator;
import com.firebase.client.MutableData;
import com.firebase.client.Query;
import com.firebase.client.ServerValue;
import com.firebase.client.Transaction;
import com.firebase.client.ValueEventListener;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;

public class Worker {
    public static final int MAX_TRANSACTION_ATTEMPTS = 10;
    public static final List<String> RESERVED = Arrays.asList(
            "_state",
            "_state_changed",
            "_owner",
            "_progress",
            "_error_details",
            "_id"
    );

    private Vertx vertx;
    private When when;
    private Logger log = LoggerFactory.getLogger(Worker.class.getName());

    private String processId = null;
    private long taskNumber = 0;
    private boolean sanitize;
    private boolean suppressStack;

    private boolean busy = false;
    private TaskSpec spec = null;

    private Deferred<String> shutdownDeferred = null;

    private Handler<TaskHandler> processingHandler;
    private Map<String, Long> expiryTimeouts = new HashMap<>();
    private Map<String, Object> owners = new HashMap<>();

    private Firebase tasksRef = null;
    private Query processingTasksRef = null;
    private Firebase currentTaskRef = null;
    private Query newTaskRef = null;

    private ValueEventListener currentTaskListener = null;
    private ChildEventListener newTaskListener = null;
    private ChildEventListener processingTaskListener = null;

    public Worker(Vertx vertx, When when, Firebase tasksRef, String processId, boolean sanitize, boolean suppressStack, Handler<TaskHandler> processingHandler) {
        this.vertx = vertx;
        this.when = when;
        this.tasksRef = tasksRef;
        this.processId = processId + ":" + UUID.randomUUID().toString();
        this.sanitize = sanitize;
        this.suppressStack = suppressStack;
        this.processingHandler = processingHandler;

        log.debug(getLogEntry("Initialized"));
    }

    public void setTaskSpec(TaskSpec newSpec) {
        this.taskNumber += 1;

        // Tear down prev
        cleanupTaskListeners();

        // the null case fails validation but there is no need to check if the snapshot was null
        if (newSpec != null && newSpec.validate()) {
            this.spec = newSpec;

            this.newTaskRef = this.tasksRef
                    .orderByChild("_state")
                    .equalTo(this.spec.getStartState())
                    .limitToFirst(1);
            log.debug(getLogEntry("listening"));
            this.newTaskListener = this.newTaskRef.addChildEventListener(new ChildEventListener() {
                @Override
                public void onChildAdded(DataSnapshot snapshot, String previousChildKey) {
                    tryToProcess();
                }

                @Override
                public void onChildChanged(DataSnapshot snapshot, String previousChildKey) {/* Not needed */ }

                @Override
                public void onChildRemoved(DataSnapshot snapshot) {/* Not needed */ }

                @Override
                public void onChildMoved(DataSnapshot snapshot, String s) {/* Not needed */ }

                @Override
                public void onCancelled(FirebaseError error) {
                    log.debug(getLogEntry("errored listening to Firebase"), error);
                }
            });
        } else {
            // In this case we now have the null task spec
            if (newSpec == null) {
                log.debug(getLogEntry("task spec removed, not listening for new tasks"));
            } else {
                log.debug(getLogEntry("invalid task spec, not listening for new tasks"));
            }

            newTaskRef = null;
            newTaskListener = null;
        }

        setUpTimeouts();
    }

    public Promise<String> shutdown() {
        log.debug(getLogEntry("shutting down"));

        // Set the global shutdown deferred promise, which signals we're shutting down
        shutdownDeferred = when.defer();

        // We can report success immediately if we're not busy
        if (!this.busy) {
            this.setTaskSpec(null);
            log.debug(getLogEntry("finished shutdown"));
            this.shutdownDeferred.resolve((String) null);
        }

        return this.shutdownDeferred.getPromise();
    }

    private String getLogEntry(String message) {
        return Thread.currentThread().getName() + ":QueueWorker " + this.processId + " " + message;
    }

    private void cleanupTaskListeners() {
        if (this.newTaskListener != null) {
            this.newTaskRef.removeEventListener(this.newTaskListener);
        }

        if (this.currentTaskListener != null) {
            log.debug(getLogEntry("released " + currentTaskRef.getKey()));
            this.currentTaskRef.child("_owner").removeEventListener(this.currentTaskListener);
            this.resetTask(this.currentTaskRef);
            this.currentTaskRef = null;
            this.currentTaskListener = null;
        }
    }

    private Promise resetTask(Firebase taskRef) {
        Deferred<String> deferred = when.defer();
        return resetTask(taskRef, deferred);
    }

    private Promise resetTask(Firebase taskRef, Deferred<String> deferred) {
        Holder<Integer> retries = new Holder<>(0);
        taskRef.runTransaction(new Transaction.Handler() {
            @Override
            public Transaction.Result doTransaction(MutableData task) {
                if (task.getValue() == null) {
                    return Transaction.success(task);
                } else if (Holder.nullSafeEquals(task.child("_state").getValue(), spec.getInProgressState())) {
                    task.child("_state").setValue(spec.getStartState());
                    task.child("_state_changed").setValue(ServerValue.TIMESTAMP);
                    task.child("_owner").setValue(null);
                    task.child("_progress").setValue(null);
                    task.child("_error_details").setValue(null);
                    return Transaction.success(task); //we can also abort by calling Transaction.abort()
                } else {
                    return Transaction.abort();
                }
            }

            @Override
            public void onComplete(FirebaseError error, boolean committed, DataSnapshot snapshot) {
                if (error != null) {
                    retries.setValue(retries.getValue() + 1);
                    if (retries.getValue() < MAX_TRANSACTION_ATTEMPTS) {
                        log.debug(getLogEntry("reset task errored, retrying"), error);
                        long timerID = vertx.setTimer(1, (Long id) -> resetTask(taskRef, deferred));
                    } else {
                        String errorMsg = "reset task errored too many times, no longer retrying";
                        log.debug(getLogEntry(errorMsg), error);
                        deferred.reject(new MaxTransactionsExceededException(errorMsg));
                    }
                } else {
                    if (committed && snapshot.exists()) {
                        log.debug(getLogEntry("reset " + snapshot.getKey()));
                    }
                    deferred.resolve((String) null);
                }
            }
        }, false);

        return deferred.getPromise();
    }

    private Promise<String> tryToProcess() {
        Deferred<String> deferred = when.defer();
        return tryToProcess(deferred);
    }

    private Promise<String> tryToProcess(Deferred<String> deferred) {
        Holder<Integer> retries = new Holder<>(0);
        Holder<Boolean> malformed = new Holder<>(false);

        if (busy) {
            deferred.resolve((String) null);
        } else if (shutdownDeferred != null) {
            deferred.reject(new Exception("Shutting down - can no longer process new tasks"));
            setTaskSpec(null);
            log.debug(getLogEntry("finished shutdown"));
            shutdownDeferred.resolve((String) null);
        } else {
            if (newTaskRef == null) {
                deferred.resolve((String) null);
            } else {
                newTaskRef.addListenerForSingleValueEvent(new ValueEventListener() {
                    @Override
                    public void onDataChange(DataSnapshot querySnapshot) {
                        if (!querySnapshot.exists()) {
                            deferred.resolve((String) null);
                            return;
                        }
                        Holder<Firebase> nextTaskRef = new Holder<>(null);
                        for (DataSnapshot childSnap : querySnapshot.getChildren()) {
                            nextTaskRef.setValue(childSnap.getRef());
                        }
                        // this is safe. snapshot must contain 1 child
                        nextTaskRef.getValue().runTransaction(new Transaction.Handler() {
                            @Override
                            public Transaction.Result doTransaction(MutableData task) {
                                if (task.getValue() == null) {
                                    return Transaction.success(task);
                                }

                                GenericTypeIndicator<Map<String, Object>> t = new GenericTypeIndicator<Map<String, Object>>() {};
                                Map<String, Object> taskData;

                                try {
                                    taskData = task.getValue(t);
                                } catch (ClassCastException e) {
                                    taskData = null;
                                }

                                if (taskData == null) {
                                    malformed.setValue(true);
                                    String errorMsg = "Task was malformed";
                                    Throwable error = new Exception(errorMsg);
                                    String errorStack = null;
                                    if (!suppressStack) {
                                        errorStack = stackTraceToString(error, null);
                                    }

                                    Map<String, Object> _error_details = new HashMap<>();
                                    _error_details.put("error", errorMsg);
                                    _error_details.put("original_task", task.getValue());
                                    _error_details.put("error_stack", errorStack);

                                    task.setValue(null);
                                    task.child("_state").setValue(spec.getErrorState());
                                    task.child("_state_changed").setValue(ServerValue.TIMESTAMP);
                                    task.child("_error_details").setValue(_error_details);

                                    return Transaction.success(task);
                                }

                                if (Holder.nullSafeEquals(task.child("_state").getValue(), spec.getStartState())) {
                                    task.child("_id").setValue(nextTaskRef.getValue().getKey());
                                    task.child("_state").setValue(spec.getInProgressState());
                                    task.child("_state_changed").setValue(ServerValue.TIMESTAMP);
                                    task.child("_owner").setValue(processId + ":" + (taskNumber + 1));
                                    task.child("_progress").setValue(0);

                                    return Transaction.success(task);
                                } else {
                                    log.debug(getLogEntry("task no longer in correct state: expected " + spec.getStartState()
                                            + ", got " + task.child("_state").getValue()));
                                    return Transaction.abort();
                                }
                            }

                            @Override
                            public void onComplete(FirebaseError error, boolean committed, DataSnapshot task) {
                                if (error != null) {
                                    retries.setValue(retries.getValue() + 1);
                                    if (retries.getValue() < MAX_TRANSACTION_ATTEMPTS) {
                                        log.debug(getLogEntry("errored while attempting to claim" +
                                                " a new task, retrying"), error);
                                        long timerID = vertx.setTimer(1, (Long id) -> tryToProcess(deferred));
                                    } else {
                                        String errorMsg = "errored while attempting to claim a new task too " +
                                                "many times, no longer retrying";
                                        log.debug(getLogEntry(errorMsg), error);
                                        deferred.reject(new MaxTransactionsExceededException(errorMsg));
                                    }

                                } else {
                                    if (committed && task.exists()) {
                                        if (malformed.getValue()) {
                                            log.debug(getLogEntry("found malformed entry " + task.getKey()));
                                        } else {
                                            if (busy) {
                                                // JWorker has become busy while the transaction was processing -
                                                // so give up the task for now so another worker can claim it
                                                resetTask(nextTaskRef.getValue());
                                            } else {
                                                busy = true;
                                                taskNumber += 1;
                                                log.debug(getLogEntry("claimed " + task.getKey()));
                                                currentTaskRef = task.getRef();
                                                currentTaskListener = currentTaskRef.child("_owner").addValueEventListener(new ValueEventListener() {
                                                    @Override
                                                    public void onDataChange(DataSnapshot ownerSnapshot) {
                                                        String id = processId + ":" + taskNumber;
                                                        if (!Holder.nullSafeEquals(ownerSnapshot.getValue(), id)
                                                                && currentTaskRef != null
                                                                && currentTaskListener != null) {
                                                            log.debug(getLogEntry("released " + task.getKey()));
                                                            currentTaskRef.child("_owner").removeEventListener(currentTaskListener);
                                                            currentTaskRef = null;
                                                            currentTaskListener = null;
                                                        }
                                                    }

                                                    @Override
                                                    public void onCancelled(FirebaseError firebaseError) {
                                                    }
                                                });
                                                GenericTypeIndicator<Map<String, Object>> t = new GenericTypeIndicator<Map<String, Object>>() {
                                                };
                                                log.debug("Calling handler for task " + task.toString());
                                                Map<String, Object> taskData = task.getValue(t);

                                                if (sanitize) {
                                                    for (String reserved : RESERVED) {
                                                        if (taskData.containsKey(reserved)) {
                                                            taskData.remove(reserved);
                                                        }
                                                    }
                                                }

                                                TaskHandler handler = new TaskHandler(taskNumber, taskData);
                                                vertx.runOnContext( (Void nothing) -> {
                                                    try {
                                                        processingHandler.handle(handler);
                                                    } catch (Exception e) {
                                                        handler.reject(e);
                                                    }
                                                });
                                            }
                                        }
                                    }
                                }
                                deferred.resolve((String) null);
                            }
                        }, false);
                    }

                    @Override
                    public void onCancelled(FirebaseError firebaseError) {
                    }
                });
            }
        }

        return deferred.getPromise();
    }

    private void setUpTimeouts() {
        if (processingTaskListener != null) {
            processingTasksRef.removeEventListener(processingTaskListener);
            processingTaskListener = null;
        }

        for (Map.Entry<String, Long> expiryTimeout : expiryTimeouts.entrySet()) {
            vertx.cancelTimer(expiryTimeout.getValue());
        }

        if (spec != null && spec.getTaskTimeout() != null && spec.getTaskTimeout() != 0) {
            processingTasksRef = tasksRef.orderByChild("_state")
                    .equalTo(spec.getInProgressState());

            Handler<DataSnapshot> setUpTimeout = (DataSnapshot snapshot) -> {
                String taskName = snapshot.getKey();
                Long now = new Date().getTime();
                Long startTime = (snapshot.child("_state_changed").getValue() instanceof Long
                        ? (Long) snapshot.child("_state_changed").getValue() : now);
                Long expires = Math.max(1, startTime - now + spec.getTaskTimeout());
                Firebase ref = snapshot.getRef();
                owners.put(taskName, snapshot.child("_owner").getValue());
                expiryTimeouts.put(taskName, vertx.setTimer(expires, (Long id) -> resetTask(ref)));
            };

            processingTasksRef.addChildEventListener(new ChildEventListener() {
                @Override
                public void onChildAdded(DataSnapshot snapshot, String previousChildKey) {
                    setUpTimeout.handle(snapshot);
                }

                @Override
                public void onChildChanged(DataSnapshot snapshot, String previousChildKey) {
                    // This catches de-duped events from the server - if the task was removed
                    // and added in quick succession, the server may squash them into a
                    // single update
                    String taskName = snapshot.getKey();
                    if (owners.containsKey(taskName) && !Holder.nullSafeEquals(owners.get(taskName), snapshot.child("_owner").getValue())) {
                        setUpTimeout.handle(snapshot);
                    }
                }

                @Override
                public void onChildRemoved(DataSnapshot snapshot) {
                    String taskName = snapshot.getKey();
                    if (expiryTimeouts.containsKey(taskName)) {
                        vertx.cancelTimer(expiryTimeouts.get(taskName));
                        expiryTimeouts.remove(taskName);
                    }
                    owners.remove(taskName);
                }

                @Override
                public void onChildMoved(DataSnapshot snapshot, String s) {/* Not needed */ }

                @Override
                public void onCancelled(FirebaseError error) {
                    log.debug(getLogEntry("errored listening to Firebase"), error);
                }
            });
        } else {
            processingTasksRef = null;
        }
    }

    private String stackTraceToString(Throwable e, Integer limitTo) {
        if (limitTo == null) limitTo = 50;
        StringBuilder sb = new StringBuilder();
        int printed = 0;
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append(element.toString());
            sb.append("\n");
            if (++printed >= limitTo) break;
        }
        return sb.toString();
    }

    public class TaskHandler {
        private Map<String, Object> taskData;

        private long taskNumber;

        private int resolveRetries = 0;
        private int rejectRetries = 0;
        private int progressRetries = 0;

        private Deferred<Boolean> resolveDeferred = when.defer();
        private Deferred<Boolean> rejectDeferred = when.defer();

        public TaskHandler(long taskNumber, Map<String, Object> taskData) {
            this.taskNumber = taskNumber;
            this.taskData = taskData;
        }

        /**
         * Resolves the current task and changes the state to the finished state.
         *
         * @param newTask
         * @return
         */
        public Promise<Boolean> resolve(Map<String, Object> newTask) {
            if ((!Holder.nullSafeEquals(this.taskNumber, Worker.this.taskNumber)) || currentTaskRef == null) {
                if (currentTaskRef == null) {
                    log.debug(getLogEntry("Can't resolve task - no task currently being processed"));
                } else {
                    log.debug(getLogEntry("Can't resolve task - no longer processing current task"));
                }
                resolveDeferred.resolve((Boolean) null);
                busy = false;
                tryToProcess();
            } else {
                Holder<Boolean> existedBefore = new Holder<>(false);
                currentTaskRef.runTransaction(new Transaction.Handler() {
                    @Override
                    public Transaction.Result doTransaction(MutableData task) {
                        existedBefore.setValue(true);
                        if (task.getValue() == null) {
                            existedBefore.setValue(false);
                            return Transaction.success(task);
                        }
                        String id = processId + ":" + taskNumber;
                        if (Holder.nullSafeEquals(task.child("_state").getValue(), spec.getInProgressState())
                                && Holder.nullSafeEquals(task.child("_owner").getValue(), id)) {

                            if (newTask != null) {
                                task.setValue(newTask);
                            }

                            if (task.hasChild("_new_state")) {
                                task.child("_state").setValue(task.child("_new_state").getValue());
                                task.child("_new_state").setValue(null);
                            }

                            Object _state = task.child("_state").getValue();
                            log.debug(getLogEntry("task complete. determining final state from " + (_state != null ? _state.toString() : "")));

                            if (_state != null && !(_state instanceof String)) {
                                if (spec.getFinishedState() == null || _state.equals(false)) {
                                    // Remove the item if no `finished_state` set in the spec or
                                    // _new_state is explicitly set to `false`.
                                    task.setValue(null);
                                    return Transaction.success(task);
                                } else {
                                    task.child("_state").setValue(spec.getFinishedState());
                                }
                            }

                            if (spec.getFinishedState() == null) {
                                task.setValue(null);
                                return Transaction.success(task);
                            } else {
                                task.child("_state").setValue(spec.getFinishedState());
                            }

                            task.child("_state_changed").setValue(ServerValue.TIMESTAMP);
                            task.child("_owner").setValue(null);
                            task.child("_progress").setValue(100);
                            task.child("_error_details").setValue(null);
                            return Transaction.success(task);
                        } else {
                            return Transaction.abort();
                        }
                    }

                    @Override
                    public void onComplete(FirebaseError error, boolean committed, DataSnapshot snapshot) {
                        if (error != null) {
                            if (++resolveRetries < MAX_TRANSACTION_ATTEMPTS) {
                                log.debug(getLogEntry("resolve task errored, retrying"), error);
                                long timerID = vertx.setTimer(1, (Long id) -> resolve(newTask));
                            } else {
                                String errorMsg = "resolve task errored too many times, no longer retrying";
                                log.debug(getLogEntry(errorMsg), error);
                                resolveDeferred.reject(new MaxTransactionsExceededException(errorMsg));
                            }
                        } else {
                            if (committed && existedBefore.getValue()) {
                                log.debug(getLogEntry("reset " + snapshot.getKey()));
                            } else {
                                log.debug(getLogEntry("Can't resolve task - current task no longer owned by this process"));
                            }
                            resolveDeferred.resolve(committed);
                            busy = false;
                            tryToProcess();
                        }
                    }
                }, false);
            }

            return resolveDeferred.getPromise();
        }

        /**
         * Rejects the current task and changes the state to spec.errorState,
         * adding additional data to the '_error_details' sub key.
         *
         * @param error
         * @return
         */
        public Promise<Boolean> reject(Object error) {
            if ((!Holder.nullSafeEquals(this.taskNumber, Worker.this.taskNumber)) || currentTaskRef == null) {
                if (currentTaskRef == null) {
                    log.debug(getLogEntry("Can't reject task - no task currently being processed"));
                } else {
                    log.debug(getLogEntry("Can't reject task - no longer processing current task"));
                }
                resolveDeferred.resolve(true);
                busy = false;
                tryToProcess();
            } else {
                Holder<String> errorString = new Holder<>(null);
                Holder<String> errorStack = new Holder<>(null);
                if (error instanceof Throwable) {
                    errorString.setValue(((Throwable) error).getMessage());
                } else if (error instanceof String) {
                    errorString.setValue((String) error);
                } else if (error != null) {
                    errorString.setValue(error.toString());
                }

                if (!suppressStack) {
                    if (error instanceof Throwable) {
                        errorStack.setValue(stackTraceToString((Throwable) error, null));
                    }
                }

                Holder<Boolean> existedBefore = new Holder<>(false);
                currentTaskRef.runTransaction(new Transaction.Handler() {
                    @Override
                    public Transaction.Result doTransaction(MutableData task) {
                        existedBefore.setValue(true);
                        if (task.getValue() == null) {
                            existedBefore.setValue(false);
                            return Transaction.success(task);
                        }

                        String id = processId + ":" + taskNumber;
                        if (Holder.nullSafeEquals(task.child("_state").getValue(), spec.getInProgressState())
                            && Holder.nullSafeEquals(task.child("_owner").getValue(), id)) {

                            Map<String, Object> _error_details = new HashMap<>();
                            int attempts = 0;
                            int currentAttempts = task.child("_error_details/attempts").getValue() instanceof Integer
                                    ? (Integer) task.child("_error_details/attempts").getValue() : 0;
                            String currentPrevState = task.child("_error_details/previous_state").getValue() instanceof String
                                    ? (String) task.child("_error_details/previous_state").getValue() : null;
                            if (currentAttempts > 0 && Holder.nullSafeEquals(currentPrevState, spec.getInProgressState())) {
                                attempts = currentAttempts;
                            }
                            if (attempts >= spec.getTaskRetries()) {
                                task.child("_state").setValue(spec.getErrorState());
                            } else {
                                task.child("_state").setValue(spec.getStartState());
                            }
                            task.child("_state_changed").setValue(ServerValue.TIMESTAMP);
                            task.child("_owner").setValue(null);
                            task.child("_error_details").setValue(null);

                            _error_details.put("previous_state", spec.getInProgressState());
                            _error_details.put("error", errorString.getValue());
                            _error_details.put("error_stack", errorStack.getValue());
                            _error_details.put("attempts", attempts + 1);
                            task.child("_error_details").setValue(_error_details);
                            return Transaction.success(task);
                        } else {
                            return Transaction.abort();
                        }
                    }

                    @Override
                    public void onComplete(FirebaseError firebaseError, boolean committed, DataSnapshot snapshot) {
                        if (firebaseError != null) {
                            if (++rejectRetries < MAX_TRANSACTION_ATTEMPTS) {
                                log.debug(getLogEntry("reject task errored, retrying"), firebaseError);
                                long timerID = vertx.setTimer(1, (Long id) -> reject(error));
                            } else {
                                String errorMsg = "reject task errored too many times, no longer retrying";
                                log.debug(getLogEntry(errorMsg), firebaseError);
                                resolveDeferred.reject(new MaxTransactionsExceededException(errorMsg));
                            }
                        } else {
                            if (committed && existedBefore.getValue()) {
                                log.debug(getLogEntry("reset " + snapshot.getKey()));
                            } else {
                                log.debug(getLogEntry("Can't reject task - current task no longer owned by this process"));
                            }
                            resolveDeferred.resolve(committed);
                            busy = false;
                            tryToProcess();
                        }
                    }
                }, false);
            }

            return rejectDeferred.getPromise();
        }

        /**
         * Updates the progress state of the task.
         *
         * @param progress
         * @return
         */
        public Promise<Boolean> updateProgress(double progress) {
            Deferred<Boolean> deferred = when.defer();

            if (progress < 0 || progress > 100) {
                deferred.reject(new IllegalArgumentException("Invalid progress. Must be in [0, 100]"));
                return deferred.getPromise();
            } else {
                return updateProgress(progress, deferred);
            }
        }

        /**
         * Updates the progress state of the task.
         *
         * @param progress
         * @return
         */
        public Promise<Boolean> updateProgress(long progress) {
            Deferred<Boolean> deferred = when.defer();

            if (progress < 0 || progress > 100) {
                deferred.reject(new IllegalArgumentException("Invalid progress. Must be in [0, 100]"));
                return deferred.getPromise();
            } else {
                return updateProgress(progress, deferred);
            }
        }

        private Promise<Boolean> updateProgress(Number progress, Deferred<Boolean> deferred) {
            if ((!Holder.nullSafeEquals(this.taskNumber, Worker.this.taskNumber)) || currentTaskRef == null) {
                String errorMsg = "Can't update progress - no task currently being processed";
                log.debug(getLogEntry(errorMsg));
                deferred.reject(new Exception(errorMsg)); // TODO: consider using a more semantic exception
                return deferred.getPromise();
            }
            currentTaskRef.runTransaction(new Transaction.Handler() {
                @Override
                public Transaction.Result doTransaction(MutableData task) {
                    if (task.getValue() == null) {
                        return Transaction.success(task);
                    }

                    String id = processId + ":" + taskNumber;
                    if (Holder.nullSafeEquals(task.child("_state").getValue(), spec.getInProgressState())
                            && Holder.nullSafeEquals(task.child("_owner").getValue(), id)) {
                        task.child("_progress").setValue(progress);
                        return Transaction.success(task);
                    } else {
                        return Transaction.abort();
                    }
                }

                @Override
                public void onComplete(FirebaseError error, boolean committed, DataSnapshot snapshot) {
                    if (error != null) {
                        if (++progressRetries < MAX_TRANSACTION_ATTEMPTS) {
                            log.debug(getLogEntry("update progress task errored, retrying"), error);
                            long timerID = vertx.setTimer(1, (Long id) -> updateProgress(progress, deferred));
                        } else {
                            progressRetries = 0;
                            String errorMsg = "update progress task errored too many times, no longer retrying";
                            log.debug(getLogEntry(errorMsg), error);
                            deferred.reject(new MaxTransactionsExceededException(errorMsg));
                        }
                    } else {
                        progressRetries = 0;
                        if (committed && snapshot.exists()) {
                            deferred.resolve(true);
                        } else {
                            String errorMsg = "Can't update progress - current task no longer owned by this process";
                            log.debug(getLogEntry(errorMsg));
                            deferred.reject(new Error(errorMsg));
                        }
                    }
                }
            }, false);

            return deferred.getPromise();
        }

        public Map<String, Object> getTaskData() {
            return taskData;
        }
    }

    // Java boiler plate getters and setters. No more significant code

    public Vertx getVertx() {
        return vertx;
    }

    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    public When getWhen() {
        return when;
    }

    public void setWhen(When when) {
        this.when = when;
    }

    public boolean isSanitize() {
        return sanitize;
    }

    public void setSanitize(boolean sanitize) {
        this.sanitize = sanitize;
    }

    public boolean isSuppressStack() {
        return suppressStack;
    }

    public void setSuppressStack(boolean suppressStack) {
        this.suppressStack = suppressStack;
    }

    public TaskSpec getSpec() {
        return spec;
    }

    public Handler<TaskHandler> getProcessingHandler() {
        return processingHandler;
    }

    public void setProcessingHandler(Handler<TaskHandler> processingHandler) {
        this.processingHandler = processingHandler;
    }

    public Firebase getTasksRef() {
        return tasksRef;
    }

    public void setTasksRef(Firebase tasksRef) {
        this.tasksRef = tasksRef;
    }
}
