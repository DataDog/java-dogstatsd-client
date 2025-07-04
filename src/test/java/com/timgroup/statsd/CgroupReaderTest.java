package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

public class CgroupReaderTest {
    private static final String dockerContainerID = "3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860";
    private static final String dockerSelfCgroup = new StringBuilder()
        .append("13:name=systemd:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("12:pids:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("11:hugetlb:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("10:net_prio:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("9:perf_event:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("8:net_cls:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("7:freezer:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("6:devices:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("5:memory:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("4:blkio:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("3:cpuacct:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("2:cpu:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n")
        .append("1:cpuset:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860")
        .toString();

    @Test
    public void containerID_parse() throws Exception {
        // Docker
        String docker = dockerSelfCgroup;
        assertThat(CgroupReader.parseSelfCgroup(docker), equalTo(dockerContainerID));

        // Kubernetes
        String kubernetes = new StringBuilder()
        .append("11:perf_event:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append("10:pids:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append("9:memory:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append("8:cpu,cpuacct:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append("7:blkio:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append("6:cpuset:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append("5:devices:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append("4:freezer:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append(" 3:net_cls,net_prio:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append("2:hugetlb:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1\n")
        .append("1:name=systemd:/kubepods/besteffort/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a/3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1")
        .toString();

        assertThat(CgroupReader.parseSelfCgroup(kubernetes), equalTo("3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1"));

        // ECS EC2
        String ecs = new StringBuilder()
        .append("9:perf_event:/ecs/name-ecs-classic/5a0d5ceddf6c44c1928d367a815d890f/38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce\n")
        .append("8:memory:/ecs/name-ecs-classic/5a0d5ceddf6c44c1928d367a815d890f/38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce\n")
        .append("7:hugetlb:/ecs/name-ecs-classic/5a0d5ceddf6c44c1928d367a815d890f/38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce\n")
        .append("6:freezer:/ecs/name-ecs-classic/5a0d5ceddf6c44c1928d367a815d890f/38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce\n")
        .append("5:devices:/ecs/name-ecs-classic/5a0d5ceddf6c44c1928d367a815d890f/38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce\n")
        .append("4:cpuset:/ecs/name-ecs-classic/5a0d5ceddf6c44c1928d367a815d890f/38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce\n")
        .append("3:cpuacct:/ecs/name-ecs-classic/5a0d5ceddf6c44c1928d367a815d890f/38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce\n")
        .append("2:cpu:/ecs/name-ecs-classic/5a0d5ceddf6c44c1928d367a815d890f/38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce\n")
        .append("1:blkio:/ecs/name-ecs-classic/5a0d5ceddf6c44c1928d367a815d890f/38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce")
        .toString();

        assertThat(CgroupReader.parseSelfCgroup(ecs), equalTo("38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce"));

        // ECS Fargate
        String ecsFargate = new StringBuilder()
        .append("11:hugetlb:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("10:pids:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("9:cpuset:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("8:net_cls,net_prio:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("7:cpu,cpuacct:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("6:perf_event:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("5:freezer:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("4:devices:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("3:blkio:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("2:memory:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .append("1:name=systemd:/ecs/55091c13-b8cf-4801-b527-f4601742204d/432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da\n")
        .toString();

        assertThat(CgroupReader.parseSelfCgroup(ecsFargate), equalTo("432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da"));

        // ECS Fargate >= 1.4.0
        String ecsFargate14 = new StringBuilder()
        .append("11:hugetlb:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("10:pids:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("9:cpuset:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("8:net_cls,net_prio:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("7:cpu,cpuacct:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("6:perf_event:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("5:freezer:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("4:devices:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("3:blkio:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("2:memory:/ecs/55091c13-b8cf-4801-b527-f4601742204d/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .append("1:name=systemd:/ecs/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n")
        .toString();

        assertThat(CgroupReader.parseSelfCgroup(ecsFargate14), equalTo("34dc0b5e626f2c5c4c5170e34b10e765-1234567890"));

        // Linux non-containerized
        String nonContainerized = new StringBuilder()
        .append("11:blkio:/user.slice/user-0.slice/session-14.scope\n")
        .append("10:memory:/user.slice/user-0.slice/session-14.scope\n")
        .append("9:hugetlb:/\n")
        .append("8:cpuset:/\n")
        .append("7:pids:/user.slice/user-0.slice/session-14.scope\n")
        .append("6:freezer:/\n")
        .append("5:net_cls,net_prio:/\n")
        .append("4:perf_event:/\n")
        .append("3:cpu,cpuacct:/user.slice/user-0.slice/session-14.scope\n")
        .append("2:devices:/user.slice/user-0.slice/session-14.scope\n")
        .append("1:name=systemd:/user.slice/user-0.slice/session-14.scope\n")
        .toString();

        assertNull(CgroupReader.parseSelfCgroup(nonContainerized));

        // Linux 4.4
        String linux44 = new StringBuilder()
        .append("11:pids:/system.slice/docker-cde7c2bab394630a42d73dc610b9c57415dced996106665d427f6d0566594411.scope\n")
        .append("1:name=systemd:/system.slice/docker-cde7c2bab394630a42d73dc610b9c57415dced996106665d427f6d0566594411.scope\n")
        .toString();

        assertThat(CgroupReader.parseSelfCgroup(linux44), equalTo("cde7c2bab394630a42d73dc610b9c57415dced996106665d427f6d0566594411"));

        // UUID
        String uuid = "1:name=systemd:/uuid/34dc0b5e-626f-2c5c-4c51-70e34b10e765\n";
        assertThat(CgroupReader.parseSelfCgroup(uuid), equalTo("34dc0b5e-626f-2c5c-4c51-70e34b10e765"));
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void assumeNotWindows() {
        Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
    }

    @Test
    public void testFilesFs() throws IOException {
        final CgroupReader.Fs fs = new CgroupReader.FilesFs();

        folder.create();
        final Path path = folder.newFolder("foo", "bar").toPath().resolve("baz");
        Files.write(path, "contents".getBytes());

        assertEquals(fs.getContents(path), "contents");
        assertEquals(fs.getInode(path), Files.getAttribute(path, "unix:ino"));

        assertThrows(IOException.class, new ThrowingRunnable() {
            @Override public void run() throws Exception {
                fs.getContents(path.resolveSibling("ook"));
            }
        });
        assertThrows(IOException.class, new ThrowingRunnable() {
            @Override public void run() throws Exception {
                fs.getInode(path.resolveSibling("ook"));
            }
        });
    }

    @Test
    public void testGetContainerID_cgroupPath() throws IOException {
        CgroupReader cr = new CgroupReader(new CgroupReader.Fs() {
            @Override public String getContents(Path path) throws IOException {
                if ("/proc/self/cgroup".equals(path.toString())) {
                    return dockerSelfCgroup;
                }
                throw new FileNotFoundException(path.toString());
            }
            @Override public long getInode(Path path) throws IOException {
                throw new IOException("not allowed");
            }
        });

        assertEquals(cr.getContainerID(), dockerContainerID);
    }

    @Test
    public void testGetContainerID_cgroupEmpty() throws IOException {
        CgroupReader cr = new CgroupReader(new CgroupReader.Fs() {
            @Override public String getContents(Path path) throws IOException {
                if ("/proc/self/cgroup".equals(path.toString())) {
                    return "";
                }
                throw new FileNotFoundException(path.toString());
            }
            @Override public long getInode(Path path) throws IOException {
                throw new IOException("not allowed");
            }
        });

        assertEquals(cr.getContainerID(), null);
    }


    @Test
    public void testGetContainerID_inodeV1() throws IOException {
        CgroupReader cr = new CgroupReader(new CgroupReader.Fs() {
            @Override public String getContents(Path path) throws IOException {
                if ("/proc/self/cgroup".equals(path.toString())) {
                    return "0:cpu:/foo\n0:memory:/foo\n";
                }
                throw new FileNotFoundException(path.toString());
            }
            @Override public long getInode(Path path) throws IOException {
                if ("/sys/fs/cgroup/memory/foo".equals(path.toString())) {
                    return 42;
                }
                throw new FileNotFoundException(path.toString());
            }
        });

        assertEquals(cr.getContainerID(), "in-42");
    }

    @Test
    public void testGetContainerID_inodeV1NonExistent() throws IOException {
        CgroupReader cr = new CgroupReader(new CgroupReader.Fs() {
            @Override public String getContents(Path path) throws IOException {
                if ("/proc/self/cgroup".equals(path.toString())) {
                    return "0:cpu:/foo\n0:memory:/foo\n";
                }
                throw new FileNotFoundException(path.toString());
            }
            @Override public long getInode(Path path) throws IOException {
                throw new FileNotFoundException(path.toString());
            }
        });

        assertEquals(cr.getContainerID(), null);
    }

    @Test
    public void testGetContainerID_inodeV2NonExistent() throws IOException {
        CgroupReader cr = new CgroupReader(new CgroupReader.Fs() {
            @Override public String getContents(Path path) throws IOException {
                if ("/proc/self/cgroup".equals(path.toString())) {
                    return "0:memory:/foo\n";
                }
                throw new FileNotFoundException(path.toString());
            }
            @Override public long getInode(Path path) throws IOException {
                throw new FileNotFoundException(path.toString());
            }
        });

        assertEquals(cr.getContainerID(), null);
    }

    @Test
    public void testGetContainerID_inodeV2() throws IOException {
        CgroupReader cr = new CgroupReader(new CgroupReader.Fs() {
            @Override public String getContents(Path path) throws IOException {
                if ("/proc/self/cgroup".equals(path.toString())) {
                    return "0::/foo\n";
                }
                throw new FileNotFoundException(path.toString());
            }
            @Override public long getInode(Path path) throws IOException {
                if ("/sys/fs/cgroup/foo".equals(path.toString())) {
                    return 42;
                }
                if ("/proc/self/ns/cgroup".equals(path)) {
                    return 0xEFFFFFFBL;
                }
                throw new FileNotFoundException(path.toString());
            }
        });

        assertEquals(cr.getContainerID(), "in-42");
    }

    @Test
    public void testGetContainerID_v2_bad_path() throws IOException {
        CgroupReader cr = new CgroupReader(new CgroupReader.Fs() {
            @Override public String getContents(Path path) throws IOException {
                if ("/proc/self/cgroup".equals(path.toString())) {
                    return "0::/foo\n";
                }
                throw new FileNotFoundException(path.toString());
            }
            @Override public long getInode(Path path) throws IOException {
                throw new FileNotFoundException(path.toString());
            }
        });

        assertEquals(cr.getContainerID(), null);
    }

    @Test
    public void testGetContainerID_inodeV2RootNs() throws IOException {
        CgroupReader cr = new CgroupReader(new CgroupReader.Fs() {
            @Override public String getContents(Path path) throws IOException {
                if ("/proc/self/cgroup".equals(path.toString())) {
                    return "0::/foo\n";
                }
                throw new FileNotFoundException(path.toString());
            }
            @Override public long getInode(Path path) throws IOException {
                if ("/proc/self/ns/cgroup".equals(path.toString())) {
                    return 0xEFFFFFFBL;
                }
                if ("/foo".equals(path.toString())) {
                    return 42;
                }
                throw new FileNotFoundException(path.toString());
            }
        });

        assertEquals(cr.getContainerID(), null);
    }

    @Test
    public void testGetContainerID_mountinfo() throws Exception {
        String[][] cases = new String[][]{
            { "docker", "0cfa82bf3ab29da271548d6a044e95c948c6fd2f7578fb41833a44ca23da425f" },
            { "kubelet", "fc7038bc73a8d3850c66ddbfb0b2901afa378bfcbb942cc384b051767e4ac6b0" },
            { "sandboxes1", null },
            { "sandboxes2", null },
        };

        for (String[] cs : cases) {
            final String mountInfoContents = getTestResource("dogstatsd/mountinfo", cs[0]);

            CgroupReader cr = new CgroupReader(new CgroupReader.Fs() {
                @Override public String getContents(Path path) throws IOException {
                    String sp = path.toString();
                    if ("/proc/self/mountinfo".equals(sp)) {
                        return mountInfoContents;
                    }
                    throw new FileNotFoundException(sp);
                }
                @Override public long getInode(Path path) throws IOException {
                    String sp = path.toString();
                    throw new FileNotFoundException(sp);
                }
            });

            assertEquals(cr.getContainerID(), cs[1]);
        }
    }

    private String getTestResource(String prefix, String name) throws Exception {
        return new String(Files.readAllBytes(Paths.get(
            getClass().getClassLoader().getResource(prefix + "/" + name).toURI())));
    }
}
