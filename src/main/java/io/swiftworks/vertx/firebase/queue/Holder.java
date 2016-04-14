package io.swiftworks.vertx.firebase.queue;

/**
 * Idea for this class came from
 * http://eclipsesource.com/blogs/2013/08/19/mutable-variable-capture-in-anonymous-java-classes/
 *
 * Use this class to get around shared state limitations of lambdas and anonymous inner classes
 */
public class Holder<T> {
    private T value;

    Holder(T value) {
        setValue(value);
    }

    T getValue() {
        return value;
    }

    void setValue(T value) {
        this.value = value;
    }

    public static boolean nullSafeEquals(Object a, Object b) {
        if (a == null && b == null) {
            return true;
        } else if (a == null || b == null) {
            return false;
        } else {
            return a.equals(b);
        }
    }
}
