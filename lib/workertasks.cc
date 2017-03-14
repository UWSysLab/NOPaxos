#include "lib/workertasks.h"

WorkerTasks::WorkerTasks()
{
    next_taskid_ = 0;
    done_ = false;
}

WorkerTasks::~WorkerTasks() { }

taskid_t
WorkerTasks::CreateTask()
{
    // Only the main transport thread can
    // call this function
    std::unique_lock<std::mutex> lck(tasks_mutex_);
    taskid_t taskid = next_taskid_++;
    task_t task;
    task.done = false;

    tasks_[taskid] = task;
    task_queue_.push_back(taskid);

    return taskid;
}

void
WorkerTasks::CompleteTask(taskid_t taskid, task_t task)
{
    std::unique_lock<std::mutex> lck(tasks_mutex_);
    task.done = true;
    tasks_[taskid] = task;
    task_cv_.notify_all();
}

task_t
WorkerTasks::PullNextCompletedTask()
{
    std::unique_lock<std::mutex> lck(tasks_mutex_);
    task_t completed_task;
    while (!done_) {
        if (!task_queue_.empty()) {
            if (tasks_[task_queue_.front()].done) {
                break;
            }
        }
        task_cv_.wait(lck);
    }
    if (!done_) {
        completed_task = tasks_[task_queue_.front()];
        tasks_.erase(task_queue_.front());
        task_queue_.pop_front();
    }
    return completed_task;
}

bool
WorkerTasks::IsEmpty()
{
    return tasks_.empty();
}

void
WorkerTasks::Stop() {
    done_ = true;
    task_cv_.notify_all();
}
