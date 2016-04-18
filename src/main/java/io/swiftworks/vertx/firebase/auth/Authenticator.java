package io.swiftworks.vertx.firebase.auth;

import com.englishtown.promises.Deferred;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.firebase.client.AuthData;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.Date;


public class Authenticator {
    private Logger log = LoggerFactory.getLogger(Authenticator.class.getName());

    private Firebase firebase;
    private When when;
    private Vertx vertx;

    private Long timerId;

    public Authenticator(Vertx vertx, When when, Firebase firebase) {
        this.vertx = vertx;
        this.when = when;
        this.firebase = firebase;
    }

    public Promise<AuthData> authenticate(TokenProvider provider) {
        Deferred<AuthData> deferred = when.defer();

        provider.getToken().then((String token) -> {
            authenticate(token, deferred);
            return null;
        });

        deferred.getPromise().then((AuthData authData) -> {
            scheduleRefresh(provider, authData);
            return when.resolve(authData);
        });

        return deferred.getPromise();
    }

    private Promise<AuthData> authenticate(String token, Deferred<AuthData> deferred) {
        firebase.authWithCustomToken(token, new Firebase.AuthResultHandler() {
            @Override
            public void onAuthenticated(AuthData authData) {
                deferred.resolve(authData);
            }

            @Override
            public void onAuthenticationError(FirebaseError error) {
                deferred.reject(error.toException());
            }
        });
        return deferred.getPromise();
    }

    private void scheduleRefresh(TokenProvider provider, AuthData authData) {
        cleanup();

        Long now = new Date().getTime();
        Long expires = authData.getExpires() - now;
        Long fiveMinutes = 5L * 60L * 1000L;
        Long refresh = expires - fiveMinutes;

        if (refresh > fiveMinutes) {
            timerId = vertx.setTimer(refresh, (Long id) -> authenticate(provider));
        } else {
            log.warn("Short Firebase Token Expiration Detected: " + expires + "ms");
        }
    }

    @PreDestroy
    void cleanup() {
        if (timerId != null) {
            vertx.cancelTimer(timerId);
            timerId = null;
        }
    }
}
