/*
 * $Id
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the BSD style
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.mule.impl.work;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;

import javax.resource.spi.work.WorkException;

/**
 * <code>WorkExecutor</code> TODO
 * 
 * @author <a href="mailto:ross.mason@symphonysoft.com">Ross Mason</a>
 * @version $Revision$
 */
public interface WorkExecutor
{

    /**
     * This method must be implemented by sub-classes in order to provide the
     * relevant synchronization policy. It is called by the executeWork template
     * method.
     * 
     * @param work Work to be executed.
     * 
     * @throws javax.resource.spi.work.WorkException Indicates that the work has
     *             failed.
     * @throws InterruptedException Indicates that the thread in charge of the
     *             execution of the specified work has been interrupted.
     */
    void doExecute(WorkerContext work, Executor executor) throws WorkException, InterruptedException;

}
