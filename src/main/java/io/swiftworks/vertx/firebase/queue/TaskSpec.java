package io.swiftworks.vertx.firebase.queue;

import com.firebase.client.DataSnapshot;

import java.util.Map;


public class TaskSpec {
    public static final String DEFAULT_START_STATE = null;
    public static final String DEFAULT_IN_PROGRESS_STATE = "in_progress";
    public static final String DEFAULT_FINISHED_STATE = null;
    public static final String DEFAULT_ERROR_STATE = "error";
    public static final Long DEFAULT_TIMEOUT = 300000L;
    public static final int DEFAULT_RETRIES = 0;


    private final String startState;
    private final String inProgressState;
    private final String finishedState;
    private final String errorState;
    private final Long taskTimeout;
    private final Integer taskRetries;

    public TaskSpec() {
        this.startState = DEFAULT_START_STATE;
        this.inProgressState = DEFAULT_IN_PROGRESS_STATE;
        this.finishedState = DEFAULT_FINISHED_STATE;
        this.errorState = DEFAULT_ERROR_STATE;
        this.taskTimeout = DEFAULT_TIMEOUT;
        this.taskRetries = DEFAULT_RETRIES;
    }

    /**
     * Marshalling is currently performed manually in order to apply default values if some keys are missing
     * TODO: test out automatic marshalling with defaults for keys not present
     *
     * @param taskSpecSnapshot
     */
    public TaskSpec(DataSnapshot taskSpecSnapshot) {
        if (taskSpecSnapshot != null && taskSpecSnapshot.exists() && taskSpecSnapshot.getValue() instanceof Map) {

            if (taskSpecSnapshot.child("start_state").exists() && taskSpecSnapshot.child("start_state").getValue() instanceof String) {
                this.startState = taskSpecSnapshot.child("start_state").getValue(String.class);
            } else {
                this.startState = DEFAULT_START_STATE;
            }

            if (taskSpecSnapshot.child("in_progress_state").exists() && taskSpecSnapshot.child("in_progress_state").getValue() instanceof String) {
                this.inProgressState = taskSpecSnapshot.child("in_progress_state").getValue(String.class);
            } else {
                this.inProgressState = DEFAULT_IN_PROGRESS_STATE;
            }

            if (taskSpecSnapshot.child("finished_state").exists() && taskSpecSnapshot.child("finished_state").getValue() instanceof String) {
                this.finishedState = taskSpecSnapshot.child("finished_state").getValue(String.class);
            } else {
                this.finishedState = DEFAULT_FINISHED_STATE;
            }

            if (taskSpecSnapshot.child("error_state").exists() && taskSpecSnapshot.child("error_state").getValue() instanceof String) {
                this.errorState = taskSpecSnapshot.child("error_state").getValue(String.class);
            } else {
                this.errorState = DEFAULT_ERROR_STATE;
            }

            // TODO: determine how Firebase returns numbers.
            if (taskSpecSnapshot.child("timeout").exists() && taskSpecSnapshot.child("timeout").getValue() instanceof Long) {
                this.taskTimeout = taskSpecSnapshot.child("timeout").getValue(Long.class);
            } else {
                this.taskTimeout = DEFAULT_TIMEOUT;
            }

            // TODO: determine how Firebase returns numbers.
            if (taskSpecSnapshot.child("retries").exists() && taskSpecSnapshot.child("retries").getValue() instanceof Integer) {
                this.taskRetries = taskSpecSnapshot.child("retries").getValue(Integer.class);
            } else {
                this.taskRetries = DEFAULT_RETRIES;
            }

        } else if (taskSpecSnapshot == null) {
            // null state - for workers that are shutting down.
            this.startState = null;
            this.inProgressState = null;
            this.finishedState = null;
            this.errorState = DEFAULT_ERROR_STATE;
            this.taskTimeout = null;
            this.taskRetries = DEFAULT_RETRIES;
        } else {
            // Default case.
            // TODO(logging): Need to log an issue or throw an exception if an invalid spec is passed in.
            //   Requires an if (taskSpecSnapshot.exists() && !(taskSpecSnapshot.getValue() instanceof Map)) clause
            this.startState = DEFAULT_START_STATE;
            this.inProgressState = DEFAULT_IN_PROGRESS_STATE;
            this.finishedState = DEFAULT_FINISHED_STATE;
            this.errorState = DEFAULT_ERROR_STATE;
            this.taskTimeout = DEFAULT_TIMEOUT;
            this.taskRetries = DEFAULT_RETRIES;
        }
    }

    boolean validate() {
        if (this.inProgressState == null) return false;
        if (this.startState != null && this.startState.equals(this.inProgressState)) return false;
        if (this.finishedState != null &&
            (
                this.finishedState.equals(this.startState) ||
                this.finishedState.equals(this.inProgressState))
            ) return false;
        if (this.errorState != null && this.errorState.equals(this.inProgressState)) return false;
        if (this.taskTimeout != null && this.taskTimeout <= 0) return false;
        if (this.taskRetries != null && this.taskRetries < 0) return false;

        return true;
    }

    public String getStartState() {
        return startState;
    }

    public String getInProgressState() {
        return inProgressState;
    }

    public String getErrorState() {
        return errorState;
    }

    public String getFinishedState() {
        return finishedState;
    }

    public Long getTaskTimeout() {
        return taskTimeout;
    }

    public int getTaskRetries() {
        return taskRetries;
    }

    @Override
    public String toString() {
        return "TaskSpec{" +
            "startState='" + startState + '\'' +
            ", inProgressState='" + inProgressState + '\'' +
            ", finishedState='" + finishedState + '\'' +
            ", errorState='" + errorState + '\'' +
            ", taskTimeout=" + taskTimeout +
            ", taskRetries=" + taskRetries +
            '}';
    }
}
