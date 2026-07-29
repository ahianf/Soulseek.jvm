// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What a consumer can see, and what it must never be handed.
 *
 * <p>Two leaks are possible once the implementation lives behind an unexported
 * package, and neither is visible in review of the file that causes it. A facet
 * method can return an internal type, which compiles here and does not compile
 * for a consumer; and a facet method can return a {@link CompletableFuture},
 * which compiles for everyone and quietly puts the async layer back on a surface
 * that exists to be blocking.
 *
 * <p>The check walks every exported type reflectively rather than reading the
 * source, so a type reachable only through a generic parameter or a nested
 * record component is caught along with the obvious ones.
 *
 * <p>Nothing here asserts about the internals. That there is no
 * {@code CompletableFuture} left in them is {@code InternalShapeTest}'s claim,
 * and it covers all of {@code src/main/java} rather than only what is exported.
 */
class ExportedSurfaceTest {

    private static final Path SOURCE = Path.of("src", "main", "java");
    private static final Pattern EXPORTS = Pattern.compile("^\\s*exports\\s+([\\w.]+)\\s*;", Pattern.MULTILINE);

    /** The packages {@code module-info.java} says are the public surface. */
    private static Set<String> exportedPackages() throws IOException {
        Set<String> packages = new TreeSet<>();
        Matcher matcher = EXPORTS.matcher(Files.readString(SOURCE.resolve("module-info.java")));
        while (matcher.find()) {
            packages.add(matcher.group(1));
        }
        return packages;
    }

    /** Every public type in an exported package, loaded. */
    private static List<Class<?>> exportedTypes() throws IOException {
        List<Class<?>> types = new ArrayList<>();
        for (String packageName : exportedPackages()) {
            Path directory = SOURCE.resolve(packageName.replace('.', '/'));
            try (Stream<Path> files = Files.list(directory)) {
                for (Path file : files.sorted().toList()) {
                    String name = file.getFileName().toString();
                    if (!name.endsWith(".java") || name.equals("package-info.java")) {
                        continue;
                    }
                    try {
                        Class<?> type = Class.forName(packageName + "." + name.substring(0, name.length() - 5));
                        if (Modifier.isPublic(type.getModifiers())) {
                            types.add(type);
                            types.addAll(Arrays.stream(type.getClasses())
                                    .filter(nested -> Modifier.isPublic(nested.getModifiers()))
                                    .toList());
                        }
                    } catch (ClassNotFoundException exception) {
                        throw new AssertionError("exported source with no class: " + file, exception);
                    }
                }
            }
        }
        return types;
    }

    /** Every class mentioned anywhere in a type, including generic arguments. */
    private static Set<Class<?>> mentioned(Type type, Set<Class<?>> seen) {
        if (type instanceof Class<?> raw) {
            seen.add(raw.isArray() ? raw.getComponentType() : raw);
        } else if (type instanceof java.lang.reflect.ParameterizedType parameterized) {
            mentioned(parameterized.getRawType(), seen);
            for (Type argument : parameterized.getActualTypeArguments()) {
                mentioned(argument, seen);
            }
        } else if (type instanceof java.lang.reflect.WildcardType wildcard) {
            Stream.concat(Arrays.stream(wildcard.getUpperBounds()), Arrays.stream(wildcard.getLowerBounds()))
                    .forEach(bound -> mentioned(bound, seen));
        } else if (type instanceof java.lang.reflect.GenericArrayType array) {
            mentioned(array.getGenericComponentType(), seen);
        }
        return seen;
    }

    /** Every type named by a public member's signature. */
    private static Set<Class<?>> signatureTypes(Class<?> type) {
        Set<Class<?>> named = new LinkedHashSet<>();
        for (Method method : type.getDeclaredMethods()) {
            if (!Modifier.isPublic(method.getModifiers()) || method.isSynthetic()) {
                continue;
            }
            mentioned(method.getGenericReturnType(), named);
            Arrays.stream(method.getGenericParameterTypes()).forEach(parameter -> mentioned(parameter, named));
            Arrays.stream(method.getGenericExceptionTypes()).forEach(thrown -> mentioned(thrown, named));
        }
        for (Constructor<?> constructor : type.getDeclaredConstructors()) {
            if (!Modifier.isPublic(constructor.getModifiers()) || constructor.isSynthetic()) {
                continue;
            }
            Arrays.stream(constructor.getGenericParameterTypes()).forEach(parameter -> mentioned(parameter, named));
        }
        for (Field field : type.getDeclaredFields()) {
            if (Modifier.isPublic(field.getModifiers()) && !field.isSynthetic()) {
                mentioned(field.getGenericType(), named);
            }
        }
        for (Type supertype : type.getGenericInterfaces()) {
            mentioned(supertype, named);
        }
        mentioned(type.getGenericSuperclass() == null ? Object.class : type.getGenericSuperclass(), named);
        return named;
    }

    @Test
    @DisplayName("no exported signature names a dev.slsk.internal type")
    void theImplementationIsNotReachableFromTheSurface() throws IOException {
        List<String> leaks = new ArrayList<>();
        for (Class<?> type : exportedTypes()) {
            for (Class<?> named : signatureTypes(type)) {
                if (named.getName().startsWith("dev.slsk.internal.")) {
                    leaks.add(type.getName() + " names " + named.getName());
                }
            }
        }
        assertTrue(
                leaks.isEmpty(),
                "the implementation is reachable from the public surface:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), leaks));
    }

    @Test
    @DisplayName("no exported signature mentions CompletableFuture")
    void theSurfaceIsBlocking() throws IOException {
        List<String> futures = new ArrayList<>();
        for (Class<?> type : exportedTypes()) {
            if (signatureTypes(type).contains(CompletableFuture.class)) {
                futures.add(type.getName());
            }
        }
        assertTrue(
                futures.isEmpty(),
                "these exported types put a future back on a blocking surface:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), futures));
    }

    @Test
    @DisplayName("the exported packages are exactly the four the module promises")
    void theExportsAreTheFourAndOnlyThose() throws IOException {
        assertEquals(Set.of("dev.slsk", "dev.slsk.events", "dev.slsk.exceptions", "dev.slsk.spi"), exportedPackages());
    }
}
