package com.timgroup.statsd;

/**
 * DirectStatsDClient is an experimental extension of {@link StatsDClient} that allows for direct access to some
 * dogstatsd features.
 *
 * <p>It is not recommended to use this client in production. This client might allow you to take advantage of
 * new features in the agent before they are released, but it might also break your application.
 */
public interface DirectStatsDClient extends StatsDClient {

    /**
     * Records values for the specified named distribution.
     *
     * <p>The method doesn't take care of breaking down the values array if it is too large. It's up to the caller to
     * make sure the size is kept reasonable.</p>
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect     the name of the distribution
     * @param values     the values to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param tags       array of tags to be added to the data
     */
    void recordDistributionValues(String aspect, double[] values, double sampleRate, String... tags);


    /**
     * Records values for the specified named distribution.
     *
     * <p>The method doesn't take care of breaking down the values array if it is too large. It's up to the caller to
     * make sure the size is kept reasonable.</p>
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect     the name of the distribution
     * @param values     the values to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param tags       array of tags to be added to the data
     */
    void recordDistributionValues(String aspect, long[] values, double sampleRate, String... tags);
}
