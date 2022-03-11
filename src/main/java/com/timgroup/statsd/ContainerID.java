package com.timgroup.statsd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A reader class that retrieves the current container ID parsed from a the cgroup file.
 *
 */
public class ContainerID {
    private static final Path CGROUP_PATH = Paths.get("/proc/self/cgroup");
    private static final String UUID_SOURCE = "[0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12}";
    private static final String CONTAINER_SOURCE = "[0-9a-f]{64}";
    private static final String TASK_SOURCE = "[0-9a-f]{32}-\\d+";
    private static final Pattern LINE_RE = Pattern.compile("(\\d+):([^:]*):(.+)$");
    private static final Pattern CONTAINER_RE =
        Pattern.compile(
            "(?:.+)?(" + UUID_SOURCE + "|" + CONTAINER_SOURCE + "|" + TASK_SOURCE + ")(?:.scope)?$");

    private static final ContainerID INSTANCE;

    public String containerID;

    public String getContainerID() {
        return containerID;
    }
    
    public void setContainerID(String containerID) {
        this.containerID = containerID;
    }

    static {
        INSTANCE = ContainerID.read(CGROUP_PATH);
    }

    public static ContainerID get() {
        return INSTANCE;
    }

    static ContainerID read(Path path) {
        final String content;
        try {
            content = new String(Files.readAllBytes(path));
        } catch (final IOException e) {
            return new ContainerID();
        }

        if (content.isEmpty()) {
            return new ContainerID();
        }

        return parse(content);
    }

    /**
     * Parses a Cgroup file content and returns the corresponding container ID.
     * 
     * @param cgroupsContent
     *     Cgroup file content
     */
    public static ContainerID parse(final String cgroupsContent) {
        final ContainerID containerID = new ContainerID();
        final String[] lines = cgroupsContent.split("\n");
        for (final String line : lines) {
            final Matcher matcher = LINE_RE.matcher(line);

            if (!matcher.matches()) {
                continue;
            }

            final String path = matcher.group(3);
            final String[] pathParts = path.split("/");

            if (pathParts.length >= 1) {
                final Matcher containerIDMatcher = CONTAINER_RE.matcher(pathParts[pathParts.length - 1]);
                if (containerIDMatcher.matches()) {
                    containerID.setContainerID(containerIDMatcher.group(1));
                    return containerID;
                }                
            }
        }
    
        return containerID;
    }
}
