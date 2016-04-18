package io.swiftworks.vertx.firebase.auth;

import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.firebase.security.token.TokenGenerator;
import com.firebase.security.token.TokenOptions;

import java.util.Map;


public class SecretTokenProvider implements TokenProvider {

    private When when;
    private Map<String, Object> data;
    private TokenOptions tokenOptions;
    private TokenGenerator tokenGenerator;

    /**
     * Creates an admin token with empty authentication data
     * @param when
     * @param secret
     */
    public SecretTokenProvider(When when, String secret) {
        this.when = when;
        data = null;
        tokenOptions = new TokenOptions();
        tokenOptions.setAdmin(true);
        tokenGenerator = new TokenGenerator(secret);
    }

    /**
     * Specify the authentication data or options.
     * @param when
     * @param secret
     * @param data
     * @param options If null is provided, the default TokenOptions constructor will be used
     */
    public SecretTokenProvider(When when, String secret, Map<String, Object> data, TokenOptions options) {
        this.when = when;
        this.data = data;
        if (options != null) {
            tokenOptions = options;
        } else {
            tokenOptions = new TokenOptions();
        }
        tokenGenerator = new TokenGenerator(secret);
    }

    @Override
    public Promise<String> getToken() {
        return when.resolve(createToken());
    }

    private String createToken() {
        return tokenGenerator.createToken(data, tokenOptions);
    }
}
