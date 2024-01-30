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
package org.kuali.common.threads.listener;

import java.io.PrintStream;

/**
 * Listener that prints a dot to System.out whenever progress is made.
 *
 * @param <T>
 */
public class ConsoleListener<T> implements ProgressListener<T> {

    PrintStream out = System.out;
    String startToken = ".";
    String completeToken = ".";
    String progressToken = ".";

    public ConsoleListener() {
        this(".", ".");
    }

    public ConsoleListener(String startToken, String completeToken) {
        super();
        this.startToken = startToken;
        this.completeToken = completeToken;
    }

    public void progressCompleted() {
        out.print(completeToken);
    }

    public void progressStarted() {
        out.print(startToken);
    }

    public void progressOccurred(int count, int total, ProgressEvent<T> event) {
        out.print(progressToken);
    }

    public PrintStream getOut() {
        return out;
    }

    public void setOut(PrintStream out) {
        this.out = out;
    }

    public String getStartToken() {
        return startToken;
    }

    public void setStartToken(String startToken) {
        this.startToken = startToken;
    }

    public String getCompleteToken() {
        return completeToken;
    }

    public void setCompleteToken(String completeToken) {
        this.completeToken = completeToken;
    }

    public String getProgressToken() {
        return progressToken;
    }

    public void setProgressToken(String progressToken) {
        this.progressToken = progressToken;
    }

}

