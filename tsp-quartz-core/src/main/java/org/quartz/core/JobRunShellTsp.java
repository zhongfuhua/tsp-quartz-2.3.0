/**
 * 
 */
package org.quartz.core;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.impl.JobExecutionContextImpl;
import org.quartz.impl.tsp.ITspProcessJob;
import org.quartz.listeners.SchedulerListenerSupport;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * JobRunShell instances are responsible for providing the 'safe' environment
 * for <code>Job</code> s to run in, and for performing all of the work of
 * executing the <code>Job</code>, catching ANY thrown exceptions, updating
 * the <code>Trigger</code> with the <code>Job</code>'s completion code,
 * etc.
 * </p>
 *
 * <p>
 * A <code>JobRunShellTsp</code> instance is created by a <code>JobRunShellFactoryTsp</code>
 * on behalf of the <code>QuartzSchedulerThread</code> which then runs the
 * shell in a thread from the configured <code>ThreadPool</code> when the
 * scheduler determines that a <code>Job</code> has been triggered.
 * </p>
 *
 * @see JobRunShellFactoryTsp
 * @see org.quartz.core.QuartzSchedulerThread
 * @see org.quartz.Job
 * @see org.quartz.Trigger
 *
 * @author James House
 */
public class JobRunShellTsp extends SchedulerListenerSupport {
    
    private final Logger              log                = LoggerFactory.getLogger(getClass());
    
    protected JobExecutionContextImpl jec                = null;
    
    protected QuartzScheduler         qs                 = null;
    
    protected TriggerFiredBundle      firedTriggerBundle = null;
    
    protected Scheduler               scheduler          = null;
    
    protected volatile boolean        shutdownRequested  = false;
    
    private JobExecutionException     jobExEx            = null;
    
    private  ITspProcessJob   tspProcessJob = null;
    
    private long                      startTime;
    
    private long                      endTime;
    
    /**
     * <p>
     * Create a TspJobRunShell instance with the given settings.
     * </p>
     *
     * @param scheduler
     *          The <code>Scheduler</code> instance that should be made
     *          available within the <code>JobExecutionContext</code>.
     */
    public JobRunShellTsp(Scheduler scheduler, TriggerFiredBundle bndle) {
        this.scheduler = scheduler;
        this.firedTriggerBundle = bndle;
    }
    
    protected void begin() throws SchedulerException {
        
    }
    
    protected void complete(boolean successfulExecution) throws SchedulerException {
        
    }
    
    @Override
    public void schedulerShuttingdown() {
        requestShutdown();
    }
    
    @Override
    protected Logger getLog() {
        return log;
    }
    
    public void requestShutdown() {
        shutdownRequested = true;
    }
    
    public void passivate() {
        jec = null;
        qs = null;
    }
    
    public boolean initialize(QuartzScheduler sched, ITspProcessJob tspProcessJob) throws SchedulerException {
        this.tspProcessJob = tspProcessJob;
        this.qs = sched;
        Job job = null;
        JobDetail jobDetail = firedTriggerBundle.getJobDetail();
        
        try {
            job = sched.getJobFactory().newJob(firedTriggerBundle, scheduler);
        } catch (SchedulerException se) {
            sched.notifySchedulerListenersError(
                "An error occured instantiating job to be executed. job= '" + jobDetail.getKey()
                    + "'",
                se);
            throw se;
        } catch (Throwable ncdfe) {
            SchedulerException se = new SchedulerException(
                "Problem instantiating class '" + jobDetail.getJobClass().getName() + "' - ",
                ncdfe);
            sched.notifySchedulerListenersError(
                "An error occured instantiating job to be executed. job= '" + jobDetail.getKey()
                    + "'",
                se);
            throw se;
        }
        this.jec = new JobExecutionContextImpl(scheduler, firedTriggerBundle, job);
        return prepareJob();
    }
    
    /**
     * 
     * 功能说明:准备任务
     * 
     * @return 准备是否成功
     */
    private boolean prepareJob(){
        qs.addInternalSchedulerListener(this);
        OperableTrigger trigger = (OperableTrigger) jec.getTrigger();
        JobDetail jobDetail = jec.getJobDetail();
        try {
            begin();
        } catch (SchedulerException se) {
            qs.notifySchedulerListenersError("Error executing Job (" + jec.getJobDetail().getKey() + ": couldn't begin execution.", se);
            return false;
        }

        try {
            if (!notifyListenersBeginning(jec)) {
                return false;
            }
        } catch(VetoedException ve) {
            try {
                CompletedExecutionInstruction instCode = trigger.executionComplete(jec, null);
                qs.notifyJobStoreJobVetoed(trigger, jobDetail, instCode);
                if (jec.getTrigger().getNextFireTime() == null) {
                    qs.notifySchedulerListenersFinalized(jec.getTrigger());
                }

                complete(true);
            } catch (SchedulerException se) {
                qs.notifySchedulerListenersError("Error during veto of Job (" + jec.getJobDetail().getKey() + ": couldn't finalize execution.", se);
            }
            return false;
        }
        startTime = System.currentTimeMillis();
        endTime = startTime;
        return true;
    }

    
    /**
     * 
     * 功能说明:完成任务
     *
     */
    public void completeJob(){
        try {
            OperableTrigger trigger = (OperableTrigger) jec.getTrigger();
            JobDetail jobDetail = jec.getJobDetail();
            endTime = System.currentTimeMillis();
            jec.setJobRunTime(endTime - startTime);
            
            if(jobExEx != null){
                getLog().info("Job " + jobDetail.getKey() + " threw a JobExecutionException: ", jobExEx);
            }
            
            if (!notifyJobListenersComplete(jec, jobExEx)) {
                return;
            }
            CompletedExecutionInstruction instCode = CompletedExecutionInstruction.NOOP;
            try {
                instCode = trigger.executionComplete(jec, jobExEx);
            } catch (Exception e) {
                SchedulerException se = new SchedulerException("Trigger threw an unhandled exception.", e);
                qs.notifySchedulerListenersError("Please report this error to the Quartz developers.", se);
            }
    
            if (!notifyTriggerListenersComplete(jec, instCode)) {
                return;
            }
    
            if (instCode == CompletedExecutionInstruction.RE_EXECUTE_JOB) {
                jec.incrementRefireCount();
                try {
                    complete(false);
                } catch (SchedulerException se) {
                    qs.notifySchedulerListenersError("Error executing Job (" + jec.getJobDetail().getKey() + ": couldn't finalize execution.", se);
                }
                refireJob();
                return ;
            }
    
            try {
                complete(true);
            } catch (SchedulerException se) {
                qs.notifySchedulerListenersError("Error executing Job (" + jec.getJobDetail().getKey() + ": couldn't finalize execution.", se);
                refireJob();
                return ;
            }
    
            qs.notifyJobStoreJobComplete(trigger, jobDetail, instCode);
        } finally {
            qs.removeInternalSchedulerListener(this);
        }
    }
    
    /**
     * 
     * 功能说明:重试任务
     *
     */
    private void refireJob(){
        JobDetail jobDetail = jec.getJobDetail();
        getLog().info("Calling refire execute on job " + jobDetail.getKey());
        boolean prepareStatus = prepareJob();
        if(prepareStatus){
            tspProcessJob.run(this);
        }
    }
    
    
    private boolean notifyListenersBeginning(JobExecutionContext jobExCtxt) throws VetoedException {
        boolean vetoed = false;
        
        // notify all trigger listeners
        try {
            vetoed = qs.notifyTriggerListenersFired(jobExCtxt);
        } catch (SchedulerException se) {
            qs.notifySchedulerListenersError(
                "Unable to notify TriggerListener(s) while firing trigger "
                    + "(Trigger and Job will NOT be fired!). trigger= "
                    + jobExCtxt.getTrigger().getKey() + " job= "
                    + jobExCtxt.getJobDetail().getKey(),
                se);
            
            return false;
        }
        
        if (vetoed) {
            try {
                qs.notifyJobListenersWasVetoed(jobExCtxt);
            } catch (SchedulerException se) {
                qs.notifySchedulerListenersError(
                    "Unable to notify JobListener(s) of vetoed execution "
                        + "while firing trigger (Trigger and Job will NOT be "
                        + "fired!). trigger= " + jobExCtxt.getTrigger().getKey() + " job= "
                        + jobExCtxt.getJobDetail().getKey(),
                    se);
                
            }
            throw new VetoedException();
        }
        
        // notify all job listeners
        try {
            qs.notifyJobListenersToBeExecuted(jobExCtxt);
        } catch (SchedulerException se) {
            qs.notifySchedulerListenersError(
                "Unable to notify JobListener(s) of Job to be executed: "
                    + "(Job will NOT be executed!). trigger= " + jobExCtxt.getTrigger().getKey()
                    + " job= " + jobExCtxt.getJobDetail().getKey(),
                se);
            
            return false;
        }
        
        return true;
    }
    
    private boolean notifyJobListenersComplete(JobExecutionContext jobExCtxt, JobExecutionException jobExEx) {
        try {
            qs.notifyJobListenersWasExecuted(jobExCtxt, jobExEx);
        } catch (SchedulerException se) {
            qs.notifySchedulerListenersError(
                "Unable to notify JobListener(s) of Job that was executed: "
                    + "(error will be ignored). trigger= " + jobExCtxt.getTrigger().getKey()
                    + " job= " + jobExCtxt.getJobDetail().getKey(),
                se);
            
            return false;
        }
        return true;
    }
    
    private boolean notifyTriggerListenersComplete(JobExecutionContext jobExCtxt,
        CompletedExecutionInstruction instCode) {
        try {
            qs.notifyTriggerListenersComplete(jobExCtxt, instCode);
            
        } catch (SchedulerException se) {
            qs.notifySchedulerListenersError(
                "Unable to notify TriggerListener(s) of Job that was executed: "
                    + "(error will be ignored). trigger= " + jobExCtxt.getTrigger().getKey()
                    + " job= " + jobExCtxt.getJobDetail().getKey(),
                se);
            
            return false;
        }
        if (jobExCtxt.getTrigger().getNextFireTime() == null) {
            qs.notifySchedulerListenersFinalized(jobExCtxt.getTrigger());
        }
        return true;
    }
    
    static class VetoedException extends Exception {
        
        private static final long serialVersionUID = 1539955697495918463L;
        
        public VetoedException() {
            
        }
    }

    /**
     * 功能说明:获取任务执行上下文
     * 
     * @return jec 任务执行上下文
     */
    public JobExecutionContext getJec() {
        return jec;
    }

    /**
     * 功能说明: 获取jobExEx
     *
     * @return jobExEx jobExEx
     */
    public JobExecutionException getJobExEx() {
        return jobExEx;
    }
    

    /**
     * 功能说明: 设置jobExEx
     *
     * @param jobExEx jobExEx
     */
    public void setJobExEx(JobExecutionException jobExEx) {
        this.jobExEx = jobExEx;
    }
    
    
    
    
}
