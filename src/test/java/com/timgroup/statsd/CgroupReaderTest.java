package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CgroupReaderTest {
    @Test
    public void containerID_parse() throws Exception {
        // Docker
        String docker = new StringBuilder()
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

        assertThat(CgroupReader.parse(docker),
                equalTo("3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860"));

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

        assertThat(CgroupReader.parse(kubernetes),
                equalTo("3e74d3fd9db4c9dd921ae05c2502fb984d0cde1b36e581b13f79c639da4518a1"));

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

        assertThat(CgroupReader.parse(ecs),
                equalTo("38fac3e99302b3622be089dd41e7ccf38aff368a86cc339972075136ee2710ce"));

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

        assertThat(CgroupReader.parse(ecsFargate),
                equalTo("432624d2150b349fe35ba397284dea788c2bf66b885d14dfc1569b01890ca7da"));

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

        assertThat(CgroupReader.parse(ecsFargate14), equalTo("34dc0b5e626f2c5c4c5170e34b10e765-1234567890"));

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

        assertNull(CgroupReader.parse(nonContainerized));

        // Linux 4.4
        String linux44 = new StringBuilder()
                .append("11:pids:/system.slice/docker-cde7c2bab394630a42d73dc610b9c57415dced996106665d427f6d0566594411.scope\n")
                .append("1:name=systemd:/system.slice/docker-cde7c2bab394630a42d73dc610b9c57415dced996106665d427f6d0566594411.scope\n")
                .toString();

        assertThat(CgroupReader.parse(linux44),
                equalTo("cde7c2bab394630a42d73dc610b9c57415dced996106665d427f6d0566594411"));
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void assumeNotWindows() {
        Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
    }

    @Test
    public void testWithExistingCgroupV1Inode() throws IOException {

        folder.create();
        Path cgroupMountPath = folder.newFolder("sys", "fs", "cgroup").toPath();
        Path controllerDir = cgroupMountPath.resolve("memory");
        Path cgroupPath = controllerDir.resolve("docker");
        Path nodePath = cgroupPath.resolve("3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860");
        Files.createDirectories(nodePath);

        long inode = (long) Files.getAttribute(nodePath, "unix:ino");

        String cgroupContent = "myfile\5:memory:/docker/3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860\n";
        String inodeResult = CgroupReader.getCgroupInode(cgroupMountPath, cgroupContent);

        assertEquals("in-" + inode, inodeResult);
    }

    @Test
    public void testWithExistingCgroupV2Inode() throws IOException {

        folder.create();
        Path cgroupMountPath = folder.newFolder("sys", "fs", "cgroup").toPath();
        Files.createDirectories(cgroupMountPath);

        long inodeNumber = (long) Files.getAttribute(cgroupMountPath, "unix:ino");
        String cgroupContent = "0::/\n";
        String inodeResult = CgroupReader.getCgroupInode(cgroupMountPath, cgroupContent);

        assertEquals("in-" + inodeNumber, inodeResult);
    }

    @Test
    public void testWithNonExistentCgroupNodePath() throws IOException {

        folder.create();
        Path cgroupMountPath = folder.newFolder("sys", "fs", "cgroup").toPath();

        String cgroupContent = "memory:/nonexistentpath\n";
        String inodeResult = CgroupReader.getCgroupInode(cgroupMountPath,
                cgroupContent);

        assertNull(inodeResult);

    }

    @Test
    public void testWithEmptyCgroupContent() throws IOException {

        folder.create();
        Path cgroupMountPath = folder.newFolder("sys", "fs", "cgroup").toPath();

        String cgroupContent = "";
        String inodeResult = CgroupReader.getCgroupInode(cgroupMountPath,
                cgroupContent);

        assertNull(inodeResult);
    }
}
