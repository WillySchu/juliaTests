type Insight
  queue
  semaphoreKey
  semaphoreTTL
  semaphoreCheckInSec
  lastWork

  function WorkerPurse(queue, semaphoreKey, semaphoreTTL)
    semaphoreCheckInSec = semaphoreTTL / 2
    lastWork = 0
    new(queue, semaphoreKey, semaphoreTTL, semaphoreCheckInSec, lastWork)
  end
end
