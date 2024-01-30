/**
 * Copyright 2004-2013 The Kuali Foundation
 *
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.opensource.org/licenses/ecl2.php
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kuali.common.threads;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;

import org.kuali.common.threads.listener.ProgressNotifier;

/**
 * Produce ThreadHandlers that create and control execution of threads for iterating over List<T> objects
 */
public class ThreadHandlerFactory {

    /**
     * Return an array of int's that represents as even of a split as possible
     *
     * For example: passing in 100,7 returns 15, 15, 14, 14, 14, 14, 14
     *
     * @param numerator
     * @param denominator
     * @return
     */
    protected int[] getDivideEvenly(int numerator, int denominator) {
        int quotient = numerator / denominator;
        int remainder = numerator % denominator;

        int[] lengths = new int[denominator];
        for (int i = 0; i < denominator; i++) {
            int length = i < remainder ? quotient + 1 : quotient;
            lengths[i] = length;
        }
        return lengths;
    }

    /**
     * Given some context, produce a ThreadHandler for iterating over the list of elements provided in the context
     *
     * @param <T>
     * @param context
     * @return
     */
    public <T> ThreadHandler<T> getThreadHandler(ThreadHandlerContext<T> context) {
        // Extract some local variables for convenience
        List<T> list = context.getList();
        int elements = list.size();
        int max = context.getMax();
        int min = context.getMin();

        // Calculate # of threads and elements per thread
        int threadCount = getThreadCount(max, min, elements, context.getDivisor());
        int[] lengths = getDivideEvenly(elements, threadCount);

        // Create a new thread handler
        ThreadHandler<T> handler = new ThreadHandler<T>();
        handler.setThreadCount(threadCount);

        // Setup a notifier/listener for tracking progress
        ProgressNotifier<T> notifier = new ProgressNotifier<T>();
        notifier.setListener(context.getListener());
        notifier.setTotal(list.size());
        handler.setNotifier(notifier);

        // Create a thread group and threads
        ThreadGroup group = new ThreadGroup("List Iterator Threads");
        group.setDaemon(true);
        Thread[] threads = getThreads(handler, list, context.getHandler(), lengths);

        // Store both in the handler
        handler.setGroup(group);
        handler.setThreads(threads);

        // Return the ThreadHandler
        return handler;
    }

    /**
     * Generate threads where each thread iterates over a portion of the list
     *
     * @param <T>
     * @param threadHandler
     * @param list
     * @param elementHandler
     * @return
     */
    protected <T> Thread[] getThreads(ThreadHandler<T> thandler, List<T> list, ElementHandler<T> ehandler, int[] lengths) {
        // Total threads we'll need
        Thread[] threads = new Thread[thandler.getThreadCount()];

        // Create each thread
        int offset = 0;
        for (int i = 0; i < threads.length; i++) {

            // Get an identifier for this thread
            int id = i + 1;

            // The number of elements this thread needs to iterate over
            int length = lengths[i];

            // Give this thread some context
            ListIteratorContext<T> context = new ListIteratorContext<T>(id, offset, length, list);
            context.setNotifier(thandler.getNotifier());
            context.setThreadHandler(thandler);
            context.setElementHandler(ehandler);

            // Create a thread and store it in the array
            Runnable runnable = new ListIteratorThread<T>(context);
            threads[i] = getThread(runnable, id, thandler.getGroup(), thandler);

            // Increment offset
            offset += length;
        }

        // Return an array of threads ready to execute
        return threads;
    }

    /**
     * Construct a Thread from the information provided
     *
     * @param <T>
     * @param runnable
     * @param id
     * @param group
     * @param handler
     * @return
     */
    protected <T> Thread getThread(Runnable runnable, int id, ThreadGroup group, UncaughtExceptionHandler handler) {
        Thread thread = new Thread(group, runnable, "ListIterator-" + id);
        thread.setUncaughtExceptionHandler(handler);
        thread.setDaemon(true);
        return thread;
    }

    /**
     * Get the number of threads to use. The number returned here will never be greater than max. It may be less than
     * min, but only in the case where elements is also less than min.
     *
     * @param max
     * @param min
     * @param elements
     * @param divisor
     * @return
     */
    protected int getThreadCount(int max, int min, int elements, int divisor) {
        // Reset min to max if needed
        min = (min > max) ? max : min;

        // Validate the params
        validate(max, min, elements, divisor);

        // Reduce max if appropriate
        max = (max > elements) ? elements : max;

        // Reduce min if appropriate
        min = (min > elements) ? elements : min;

        // Divisor allows clients to scale threads in proportion to the number of elements in the list
        int threads = (divisor > 0) ? (elements / divisor) : max;

        // Reset to max if we have exceeded it
        threads = (threads > max) ? max : threads;

        // Reset to min if we have dropped below it
        threads = (threads < min) ? min : threads;

        // Return the thread count to use
        return threads;
    }

    protected void validate(int max, int min, int elements, int divisor) {
        StringBuilder sb = new StringBuilder();
        if (divisor < 0) {
            sb.append(" [divisor must be >= 0] ");
        }
        if (max < 1) {
            sb.append(" [max must be >= 1] ");
        }
        if (min < 0) {
            sb.append(" [min must be >= 0] ");
        }
        if (elements < 0) {
            sb.append(" [elements must be >= 0] ");
        }
        if (min > max) {
            sb.append(" [min must be <= max] ");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

}