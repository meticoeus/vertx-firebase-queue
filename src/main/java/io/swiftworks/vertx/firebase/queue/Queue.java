package io.swiftworks.vertx.firebase.queue;

import com.englishtown.promises.Deferred;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.firebase.client.DataSnapshot;

import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.ValueEventListener;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Queue {
    public static final int DEFAULT_NUM_WORKERS = 1;
    public static final boolean DEFAULT_SANITIZE = true;
    public static final boolean DEFAULT_SUPPRESS_STACK = true;
    public static final TaskSpec DEFAULT_TASK_SPEC = new TaskSpec();

    private Vertx vertx;
    private When when;
    Logger log = LoggerFactory.getLogger(Queue.class.getName());

    private int numWorkers = DEFAULT_NUM_WORKERS;
    private boolean sanitize = DEFAULT_SANITIZE;
    private boolean suppressStack = DEFAULT_SUPPRESS_STACK;
    private boolean initialized = false;

    private String specId;
    private Firebase tasksRef = null;
    private Firebase specsRef = null;
    private ValueEventListener specChangeListener = null;
    private List<Worker> workers = null;
    private Handler<Worker.TaskHandler> processingHandler = null;


    /**
     * Added for DI support.
     * Requires injecting everything through setters.
     */
    public Queue() {

    }

    public Queue(Vertx vertx, When when, Handler<Worker.TaskHandler> processingHandler) {
        this.vertx = vertx;
        this.when = when;
        this.processingHandler = processingHandler;
    }

    public Queue(Vertx vertx, When when, Firebase queueRef, Handler<Worker.TaskHandler> processingHandler) {
        this.vertx = vertx;
        this.when = when;
        this.processingHandler = processingHandler;
        setQueueRef(queueRef);
    }

    public Queue(Vertx vertx, When when, Firebase tasksRef, Firebase specsRef, Handler<Worker.TaskHandler> processingHandler) {
        this.vertx = vertx;
        this.when = when;
        this.processingHandler = processingHandler;
        this.tasksRef = tasksRef;
        this.specsRef = specsRef;
    }

    public void setQueueRef(Firebase ref) {
        tasksRef = ref.child("tasks");
        specsRef = ref.child("specs");
    }

    /**
     * Call after setting any custom options you want to use
     *
     * Example:
     * Queue queue = new Queue(vertx, when, processingHandler);
     * queue.setQueueRef(new Firebase('https://<your-firebase>.firebaseio.com/queue'));
     * queue.setSpecId("custom_task");
     * queue.setSanitize(false);
     * queue.start();
     */
    public Promise<String> start() {
        if (initialized || specChangeListener != null) throw new RuntimeException("Queue has already been started!");
        if (vertx == null) throw new RuntimeException("Queue vertx must be injected!");
        if (when == null) throw new RuntimeException("Queue when must be injected!");
        if (processingHandler == null) throw new RuntimeException("Queue processingHandler is required!");
        if (tasksRef == null) throw new RuntimeException("Queue tasksRef is required!");
        if (specId != null && specsRef == null) throw new RuntimeException("Queue specsRef is required if specId is provided!");

        workers = new ArrayList<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            String processId = (specId != null ? specId + ":" : "") + i;
            workers.add(new Worker(vertx, when, tasksRef, processId, sanitize, suppressStack, processingHandler));
        }

        System.out.println("Initialized workers: " + numWorkers);
        return watchSpec();
    }

    public Promise shutdown() {
        log.info("Queue: Shutting down");

        if (specChangeListener != null) {
            specsRef.child(specId).removeEventListener(specChangeListener);
            specChangeListener = null;
        }

        List<Promise<String>> promises = new ArrayList<>();

        for (Worker worker : workers) {
            promises.add(worker.shutdown());
        }

        return when.all(promises);
    }

    private Promise<String> watchSpec() {
        Deferred<String> deferred = when.defer();

        if (specId == null) {
            log.info("Queue: Assigned Default Spec");
            for (Worker worker : workers) {
                worker.setTaskSpec(DEFAULT_TASK_SPEC);
            }
            deferred.resolve((String) null);
            initialized = true;
        } else {
            specChangeListener = specsRef.child(specId).addValueEventListener(new ValueEventListener() {
                @Override
                public void onDataChange(DataSnapshot taskSpecSnapshot) {
                    log.info("Queue: Assigned Updated Spec: " + taskSpecSnapshot.getKey());

                    TaskSpec taskSpec = new TaskSpec(taskSpecSnapshot);

                    for (Worker worker : workers) {
                        worker.setTaskSpec(taskSpec);
                    }

                    if (!initialized) {
                        deferred.resolve((String) null);
                        initialized = true;
                    }
                }

                @Override
                public void onCancelled(FirebaseError firebaseError) {
                    if (!initialized) {
                        deferred.reject(firebaseError.toException());
                        initialized = true;
                    }
                }
            });
        }

        return deferred.getPromise();
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

    public int getNumWorkers() {
        return numWorkers;
    }

    public void setNumWorkers(int numWorkers) {
        this.numWorkers = numWorkers;
    }

    public boolean isSuppressStack() {
        return suppressStack;
    }

    public void setSuppressStack(boolean suppressStack) {
        this.suppressStack = suppressStack;
    }

    public String getSpecId() {
        return specId;
    }

    public void setSpecId(String specId) {
        this.specId = specId;
    }

    public Firebase getTasksRef() {
        return tasksRef;
    }

    public void setTasksRef(Firebase tasksRef) {
        this.tasksRef = tasksRef;
    }

    public Firebase getSpecsRef() {
        return specsRef;
    }

    public void setSpecsRef(Firebase specsRef) {
        this.specsRef = specsRef;
    }

    public Handler<Worker.TaskHandler> getProcessingHandler() {
        return processingHandler;
    }

    public void setProcessingHandler(Handler<Worker.TaskHandler> processingHandler) {
        this.processingHandler = processingHandler;
    }
}
