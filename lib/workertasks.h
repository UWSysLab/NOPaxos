/**
 *  lib/workertasks.h
 *  A work queue for multi-threaded transports. The main
 *  transport thread reads network packets and create a
 *  task (ordered). The main thread assign the task to a
 *  worker thread which deserialize the packet. After the
 *  worker is done, it writes the deserialized packet into
 *  the task and indicates the task is done. The replication
 *  thread reads finished tasks in the order assigned by
 *  the main thread.
 *
 *  @author Jialin Li.
 */

#ifndef _LIB_WORKERTASKS_H_
#define _LIB_WORKERTASKS_H_

#include <unordered_map>
#include <map>
#include <list>
#include <mutex>
#include <condition_variable>
#include "transport.h"

typedef uint64_t taskid_t;

struct task_t {
    TransportReceiver *receiver;
    TransportAddress *remote;
    string type;
    string data;
    uint64_t sid;
    uint64_t msgid;
    std::map<uint32_t, uint64_t> *rid_msgid;
    bool done;
};

class WorkerTasks {
public:
    WorkerTasks();
    ~WorkerTasks();

    taskid_t CreateTask();
    void CompleteTask(taskid_t taskid, task_t task);
    task_t PullNextCompletedTask();
    bool IsEmpty();
    void Stop();

private:
    std::unordered_map<taskid_t, task_t> tasks_;
    std::list<taskid_t> task_queue_;
    taskid_t next_taskid_;

    std::mutex tasks_mutex_;
    std::condition_variable task_cv_;
    bool done_;
};

#endif /* _LIB_WORKERTASKS_H_ */
