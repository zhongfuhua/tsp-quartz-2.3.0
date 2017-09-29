/**
 * 
 */
package org.quartz.impl.tsp;

import org.quartz.core.JobRunShellTsp;

/**
 * 功能说明:
 *
 * @author ZHONGFUHUA
 * 
 * @since 
 *
 */
public interface ITspProcessJob {
    
    public void run(JobRunShellTsp jobRunShell);
    
}
