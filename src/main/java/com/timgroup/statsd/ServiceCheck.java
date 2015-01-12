package com.timgroup.statsd;

/**
 * A service check model, which is used to format a service check message
 * sent to the datadog agent
 */
public class ServiceCheck {

    private String name, hostname, message;

    private int status, checkRunId, timestamp;

    private String[] tags;

    /**
     */
    public ServiceCheck() {
    }

    /**
     * @param name
     * @param status
     */
    public ServiceCheck(String name, int status) {
            this.name = name;
            this.status = status;
    }

    /**
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return
     */
    public int getStatus() {
        return status;
    }

    /**
     * @param status
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * @return
     */
    public String getMessage() {
        return message;
    }

    /**
     * @return
     */
    public String getEscapedMessage() {
        return message.replace("\n", "\\n").replace("m:", "m\\:");
    }

    /**
     *
     * @param message
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @return
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * @param hostname
     */
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    /**
     * @return
     */
    public int checkRunId() {
        return checkRunId;
    }

    /**
     * @param checkRunId
     */
    public void setCheckRunId(int checkRunId) {
        this.checkRunId = checkRunId;
    }

    /**
     * @return
     */
    public int getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp
     */
    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return
     */
    public String[] getTags() {
        return tags;
    }

    /**
     * @param tags
     */
    public void setTags(String... tags) {
        this.tags = tags;
    }

    /**
     * @return
     */
    public String toStatsDString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("_sc|%s|%d", name, status));
        if (timestamp > 0) {
            sb.append(String.format("|d:%d", timestamp));
        }
        if (checkRunId > 0) {
            sb.append(String.format("|i:%d", checkRunId));
        }
        if (hostname != null) {
            sb.append(String.format("|h:%s", hostname));
        }
        if (tags != null && tags.length > 0) {
            sb.append(String.format("|#%s", tags[0]));
            for(int i=1;i<tags.length;i++) {
                sb.append(',').append(tags[i]);
            }
            sb.append(String.format("", hostname));
        }
        if (message != null) {
            sb.append(String.format("|m:%s", this.getEscapedMessage()));
        }
        return sb.toString();
    }

    public String test() {
        return "Test";
    }
}
