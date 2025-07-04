package com.timgroup.statsd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A reader class that retrieves the current container ID or the cgroup controller
 * inode parsed from the cgroup file.
 *
 */
class CgroupReader {
    private static final Path CGROUP_PATH = Paths.get("/proc/self/cgroup");
    private static final String UUID_SOURCE = "[0-9a-f]{8}(?:[_-][0-9a-f]{4}){3}[_-][0-9a-f]{12}";
    private static final String CONTAINER_SOURCE = "[0-9a-f]{64}";
    private static final String TASK_SOURCE = "[0-9a-f]{32}-\\d+";
    private static final Pattern LINE_RE = Pattern.compile("^\\d+:[^:]*:(.+)$", Pattern.MULTILINE | Pattern.UNIX_LINES);
    private static final Pattern CONTAINER_RE = Pattern.compile(
            "(" + UUID_SOURCE + "|" + CONTAINER_SOURCE + "|" + TASK_SOURCE + ")(?:.scope)?$");

    /**
     * DEFAULT_CGROUP_MOUNT_PATH is the default cgroup mount path.
     **/
    private static final Path DEFAULT_CGROUP_MOUNT_PATH = Paths.get("/sys/fs/cgroup");
    /**
     * CGROUP_NS_PATH is the path to the cgroup namespace file.
     **/
    private static final Path CGROUP_NS_PATH = Paths.get("/proc/self/ns/cgroup");

    /**
     * CGROUPV1_BASE_CONTROLLER is the controller used to identify the container-id
     * in cgroup v1 (memory).
     **/
    private static final String CGROUPV1_BASE_CONTROLLER = "memory";
    /**
     * CGROUPV2_BASE_CONTROLLER is the controller used to identify the container-id
     * in cgroup v2.
     **/
    private static final String CGROUPV2_BASE_CONTROLLER = "";
    /**
     * HOST_CGROUP_NAMESPACE_INODE is the inode of the host cgroup namespace.
     **/
    private static final long HOST_CGROUP_NAMESPACE_INODE = 0xEFFFFFFBL;

    private final Path MOUNTINFO_PATH = Paths.get("/proc/self/mountinfo");

    private final Pattern MOUNTINFO_RE = Pattern.compile(
        ".*/([^\\s/]+)/(([0-9a-f]{64})|([0-9a-f]{32}-\\d+)|([0-9a-f]{8}(-[0-9a-f]{4}){4})$)/[\\S]*hostname"
    );

    interface Fs {
        String getContents(Path path) throws IOException;

        long getInode(Path path) throws IOException;
    }

    static class FilesFs implements Fs {
        @Override
        public String getContents(Path path) throws IOException {
            return new String(Files.readAllBytes(path));
        }

        @Override
        public long getInode(Path path) throws IOException {
            return (long) Files.getAttribute(path, "unix:ino");
        }
    }

    private final Fs fs;

    CgroupReader() {
        this(new FilesFs());
    }

    CgroupReader(Fs fs) {
        super();
        this.fs = fs;
    }

    /**
     * Returns the container ID if available or the cgroup controller inode.
     *
     * @throws IOException if /proc/self/cgroup is readable and still an I/O error
     *                     occurs reading from the stream.
     */
    public String getContainerID() throws IOException {
        String containerID = null;

        String cgroupContent = null;
        try {
            cgroupContent = fs.getContents(CGROUP_PATH);
        } catch (IOException ex) {
            // ignored
        }

        if (!isEmpty(cgroupContent)) {
            containerID = parseSelfCgroup(cgroupContent);
        }

        if (!isEmpty(containerID)) {
            return containerID;
        }

        try {
            containerID = trySelfMountInfo();
        } catch (IOException ex) {
            // ignored
        }

        if (!isEmpty(containerID)) {
            return containerID;
        }

        /*
         * If the container ID is not available it means that the application is either
         * not running in a container or running is private cgroup namespace, we
         * fallback to the cgroup controller inode. The agent (7.51+) will use it to get
         * the container ID.
         * In Host cgroup namespace, the container ID should be found. If it is not
         * found, it means that the application is running on a host/vm.
         *
         */
        if (!isEmpty(cgroupContent) && !isHostCgroupNamespace(CGROUP_NS_PATH)) {
            containerID = getCgroupInode(DEFAULT_CGROUP_MOUNT_PATH, cgroupContent);
        }
        return containerID;
    }

    /**
     * Parses a Cgroup file (=/proc/self/cgroup) content and returns the
     * corresponding container ID. It can be found only if the container
     * is running in host cgroup namespace.
     *
     * @param cgroupsContent Cgroup file content
     */
    public static String parseSelfCgroup(final String cgroupsContent) {
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

    /**
     * Returns true if the host cgroup namespace is used.
     * It looks at the inode of `/proc/self/ns/cgroup` and compares it to
     * HOST_CGROUP_NAMESPACE_INODE.
     *
     * @param path Path to the cgroup namespace file.
     */
    private boolean isHostCgroupNamespace(final Path path) {
        long hostCgroupInode = inodeForPath(path);
        return hostCgroupInode == HOST_CGROUP_NAMESPACE_INODE;
    }

    /**
     * Returns the inode for the given path.
     *
     * @param path Path to the cgroup namespace file.
     */
    private long inodeForPath(final Path path) {
        try {
            long inode = (long) fs.getInode(path);
            return inode;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Returns the cgroup controller inode for the given cgroup mount path and
     * procSelfCgroupPath.
     *
     * @param cgroupMountPath Path to the cgroup mount point.
     * @param cgroupContent   String content of the cgroup file.
     */
    public String getCgroupInode(final Path cgroupMountPath, final String cgroupContent) throws IOException {
        Map<String, String> cgroupControllersPaths = parseCgroupNodePath(cgroupContent);
        if (cgroupControllersPaths == null) {
            return null;
        }

        // Retrieve the cgroup inode from /sys/fs/cgroup+controller+cgroupNodePath
        List<String> controllers = Arrays.asList(CGROUPV1_BASE_CONTROLLER, CGROUPV2_BASE_CONTROLLER);
        for (String controller : controllers) {
            String cgroupNodePath = cgroupControllersPaths.get(controller);
            if (cgroupNodePath == null) {
                continue;
            }
            Path path = Paths.get(cgroupMountPath.toString(), controller, cgroupNodePath);
            long inode = inodeForPath(path);
            /*
             * Inode 0 is not a valid inode. Inode 1 is a bad block inode and inode 2 is the
             * root of a filesystem. We can safely ignore them.
             */
            if (inode > 2) {
                return "in-" + inode;
            }
        }

        return null;
    }

    /**
     * Returns a map of cgroup controllers and their corresponding cgroup path.
     *
     * @param cgroupContent Cgroup file content.
     */
    public Map<String, String> parseCgroupNodePath(final String cgroupContent) throws IOException {
        Map<String, String> res = new HashMap<>();
        BufferedReader br = new BufferedReader(new StringReader(cgroupContent));

        String line;
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(":");
            if (tokens.length != 3) {
                continue;
            }
            if (CGROUPV1_BASE_CONTROLLER.equals(tokens[1]) || CGROUPV2_BASE_CONTROLLER.equals(tokens[1])) {
                res.put(tokens[1], tokens[2]);
            }
        }

        br.close();
        return res;
    }

    private static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    String trySelfMountInfo() throws IOException {
        String mountInfo = fs.getContents(MOUNTINFO_PATH);
        if (isEmpty(mountInfo)) {
            return null;
        }

        BufferedReader br = new BufferedReader(new StringReader(mountInfo));

        String line;
        while ((line = br.readLine()) != null) {
            Matcher matcher = MOUNTINFO_RE.matcher(line);
            if (matcher.find()) {
                if (!"sandboxes".equals(matcher.group(1))) {
                    return matcher.group(2);
                }
            }
        }

        return null;
    }
}
