package net.greghaines.jesque.worker;

import static net.greghaines.jesque.worker.JobExecutor.State.RUNNING;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_POLL;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;

public class WorkerExitOnEmpty extends WorkerImpl {
    private int maxLoopsOnEmptyQueues;

    public WorkerExitOnEmpty(final Config config, final Collection<String> queues, final JobFactory jobFactory) {
        this(config, queues, jobFactory, 3);
    }

    public WorkerExitOnEmpty(final Config config, final Collection<String> queues, final JobFactory jobFactory,
        int maxLoopsOnEmptyQueues) {
        super(config, queues, jobFactory);
        this.maxLoopsOnEmptyQueues = maxLoopsOnEmptyQueues;
    }

    /* (non-Javadoc)
     * @see net.greghaines.jesque.worker.WorkerImpl#poll()
     * Exits if all queues are empty maxLoopOnEmptyQueues times
     */
    @Override
    protected void poll() {
      int missCount = 0;
      String curQueue = null;
      int allQueuesEmptyCount = 0;
      
      while (RUNNING.equals(this.state.get())) {
          try {
              if (isThreadNameChangingEnabled()) {
                  renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
              }
              curQueue = this.queueNames.poll(EMPTY_QUEUE_SLEEP_TIME, TimeUnit.MILLISECONDS);
              if (curQueue != null) {
                  this.queueNames.add(curQueue); // Rotate the queues
                  checkPaused(); 
                  // Might have been waiting in poll()/checkPaused() for a while
                  if (RUNNING.equals(this.state.get())) {
                      this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
                      final String payload = pop(curQueue);
                      if (payload != null) {
                          final Job job = ObjectMapperFactory.get().readValue(payload, Job.class);
                          process(job, curQueue);
                          missCount = 0;
                          allQueuesEmptyCount = 0;
                      } else if (++missCount >= this.queueNames.size() && RUNNING.equals(this.state.get())) {
                          // Keeps worker from busy-spinning on empty queues
                          missCount = 0;
                          Thread.sleep(EMPTY_QUEUE_SLEEP_TIME);
                          
                          allQueuesEmptyCount++;
                          if (allQueuesEmptyCount >= maxLoopsOnEmptyQueues) {
                              end(false); // sets state to SHUTDOWN which will break the loop
                          }
                      }
                  }
              }
          } catch (InterruptedException ie) {
              if (!isShutdown()) {
                  recoverFromException(curQueue, ie);
              }
          } catch (Exception e) {
              recoverFromException(curQueue, e);
          }
      }
    }
}
