package com.firebase.client;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.ScheduledFuture;

/**
 * Inspired by https://github.com/hudak/mod-firebase-connector
 *
 * @author meticoeus
 */
public class VertxFirebaseConfigurator {
    private final Vertx vertx;
    private final Context context;
    private final io.vertx.core.logging.Logger logger;


    public VertxFirebaseConfigurator(Vertx vertx) {
        this.vertx = vertx;
        this.context = vertx.getOrCreateContext();
        this.logger = LoggerFactory.getLogger(Firebase.class.getName());
        Config config = Firebase.getDefaultConfig();
        config.setLogger(new VertxLogger());
        config.setEventTarget(new VertxRunner());
        config.setRunLoop(new VertxRunner());
        Firebase.setDefaultConfig(config);
    }

    private class VertxRunner implements RunLoop, EventTarget {
        @Override
        public void scheduleNow(final Runnable runnable) {
            context.runOnContext((Void nothing) -> {
                try {
                    runnable.run();
                } catch (Exception e) {
                    logger.error("Uncaught exception from Firebase", e);
                }
            });
        }

        @Override
        public ScheduledFuture schedule(final Runnable runnable, long delay) {
            VertxScheduledRunnable future = new VertxScheduledRunnable(vertx, delay, runnable);
            scheduleNow(future);
            return future;
        }

        @Override
        public void postEvent(Runnable runnable) {
            scheduleNow(runnable);
        }

        @Override
        public void shutdown() {
            logger.warn("Vertx Runner asked to shut down");
        }

        @Override
        public void restart() {
            logger.warn("Vertx Runner asked to restart");
        }
    }

    private class VertxLogger implements Logger {
        @Override
        public void onLogMessage(Level level, String tag, String message, long timestamp) {
            switch (level) {
                case DEBUG:
                    logger.debug(message);
                    break;
                case INFO:
                    logger.info(message);
                    break;
                case WARN:
                    logger.warn(message);
                    break;
                case ERROR:
                    logger.error(message);
                    break;
            }
        }

        @Override
        public Level getLogLevel() {
            if (logger.isDebugEnabled()) {
                return Level.DEBUG;
            } else if (logger.isInfoEnabled()) {
                return Level.INFO;
            } else {
                return Level.WARN;
            }
        }
    }
}
