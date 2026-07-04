package com.ourgiant.dynamodb.browser;

import org.objenesis.ObjenesisStd;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Test helper for exercising DynamoDBBrowser's private, self-contained helper methods
 * without running its constructor (which shows dialogs and can call System.exit).
 */
final class ReflectionSupport {

    private static final ObjenesisStd OBJENESIS = new ObjenesisStd();

    private ReflectionSupport() {
    }

    static DynamoDBBrowser newBrowserInstance() {
        return OBJENESIS.newInstance(DynamoDBBrowser.class);
    }

    static void setField(Object target, String fieldName, Object value) {
        try {
            Field field = DynamoDBBrowser.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError("Reflection setup failed for field " + fieldName, e);
        }
    }

    static Object invoke(Object target, String methodName, Class<?>[] paramTypes, Object... args) {
        try {
            Method method = DynamoDBBrowser.class.getDeclaredMethod(methodName, paramTypes);
            method.setAccessible(true);
            return method.invoke(target, args);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new AssertionError("Reflection setup failed for " + methodName, e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new AssertionError("Invocation of " + methodName + " failed", cause);
        }
    }
}
