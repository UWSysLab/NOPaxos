#include "lib/workertasks.h"
#include "ctpl/ctpl_stl.h"
#include <thread>
#include <gtest/gtest.h>

class WorkerTasksTest : public ::testing::Test
{
protected:
    virtual void SetUp() override {
        worker_pool = new ctpl::thread_pool(4);
        tasks = new WorkerTasks();
    }

    virtual void TearDown() override {
        delete tasks;
        delete worker_pool;
    }

    ctpl::thread_pool *worker_pool;
    WorkerTasks *tasks;
};

TEST_F(WorkerTasksTest, SimpleTasksTest)
{
    std::thread client([=](){
        for (int i = 0; i < 10; i++) {
            task_t task = this->tasks->PullNextCompletedTask();
            EXPECT_TRUE(task.done);
            EXPECT_EQ(task.sid, 0);
            EXPECT_EQ(task.msgid, i);
        }
    });

    for (int i = 0; i < 10; i++) {
        taskid_t taskid = tasks->CreateTask();
        worker_pool->push([=](int fd){
            task_t task;
            task.sid = 0;
            task.msgid = taskid;
            this->tasks->CompleteTask(taskid, task);
        });
    }

    client.join();
    EXPECT_TRUE(tasks->IsEmpty());
}
