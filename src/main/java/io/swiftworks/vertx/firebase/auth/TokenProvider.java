package io.swiftworks.vertx.firebase.auth;

import com.englishtown.promises.Promise;


public interface TokenProvider {
    Promise<String> getToken();
}
