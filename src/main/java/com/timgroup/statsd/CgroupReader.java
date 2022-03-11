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
public class CgroupReader {
    private static final Path CGROUP_PATH = Paths.get("/proc/self/cgroup");
    private static final String CONTAINER_SOURCE = "[0-9a-f]{64}";
    private static final String TASK_SOURCE = "[0-9a-f]{32}-\\d+";
    private static final Pattern LINE_RE = Pattern.compile("^\\d+:[^:]*:(.+)$", Pattern.MULTILINE | Pattern.UNIX_LINES);
    private static final Pattern CONTAINER_RE =
        Pattern.compile(
            "(" + CONTAINER_SOURCE + "|" + TASK_SOURCE + ")(?:.scope)?$");

    private boolean readOnce = false;
    public String containerID;

    /**
     * Parses /proc/self/cgroup and returns the container ID if available.
     * 
     * @throws IOException
     *     if /proc/self/cgroup is readable and still an I/O error occurs reading from the stream
     */
    public String getContainerID() throws IOException {
        if (readOnce) {
            return containerID;
        }

        containerID = read(CGROUP_PATH);
        return containerID;
    }

    private String read(Path path) throws IOException {
        readOnce = true;
        if (!Files.isReadable(path)) {
            return null;
        }

        final String content = new String(Files.readAllBytes(path));
        if (content.isEmpty()) {
            return null;
        }

        return parse(content);
    }

    /**
     * Parses a Cgroup file content and returns the corresponding container ID.
     * 
     * @param cgroupsContent
     *     Cgroup file content
     */
    public static String parse(final String cgroupsContent) {
        final Matcher lines = LINE_RE.matcher(cgroupsContent);
        while (lines.find()) {
            final String path = lines.group(1);
            final Matcher matcher = CONTAINER_RE.matcher(path);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }

        return null;
    }
}
