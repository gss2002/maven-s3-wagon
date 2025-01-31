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

/**
 * Listener that does nothing
 *
 * @param <T>
 */
public class NoOpListener<T> implements ProgressListener<T> {

    public void progressCompleted() {
        ; // do nothing
    }

    public void progressStarted() {
        ; // do nothing
    }

    public void progressOccurred(int count, int total, ProgressEvent<T> event) {
        ; // do nothing
    }

}