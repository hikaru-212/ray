#include "baseline-fixture.cc"
#include <array>
#include <iostream>
#include <map>
#include <mutex>

namespace ray { namespace core {
namespace {
void ProductionMinusOneSchedule(bool separate_groups) {
  instrumented_io_context io;
  ActorTaskExecutionArgWaiter waiter(
      [](const std::vector<rpc::ObjectReference> &, const TaskID &, int64_t) {});
  FakeTaskEventBuffer events;
  ray::observability::FakeRayEventRecorder recorder;
  std::vector<ConcurrencyGroup> groups;
  if (separate_groups) groups = {{"g", 1, {}}, {"h", 1, {}}};
  auto pools = std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(groups);
  std::mutex mu;
  std::array<int, 3> executed{}, canceled{}, completed{};
  std::array<rpc::PushTaskReply, 3> replies;
  std::array<std::promise<void>, 3> done;
  std::array<std::future<void>, 3> futures{
      done[0].get_future(), done[1].get_future(), done[2].get_future()};
  std::vector<int> order;
  auto index = [](const TaskToExecute &task) {
    return std::stoi(task.TaskSpec().GetMessage().name());
  };
  UnorderedActorTaskExecutionQueue queue(
      io, waiter, events, recorder, pools, nullptr, false, 1, groups,
      [&](TaskToExecute &task) {
        const auto i = index(task);
        { std::lock_guard<std::mutex> lock(mu); ++executed[i]; order.push_back(i); }
        task.send_reply_callback()(Status::OK(), nullptr, nullptr);
      },
      [&](const TaskToExecute &task, const Status &status) {
        const auto i = index(task);
        { std::lock_guard<std::mutex> lock(mu); ++canceled[i]; }
        task.send_reply_callback()(status, nullptr, nullptr);
      });
  std::array<TaskSpecification, 3> specs;
  for (int i=0; i<3; ++i) {
    const int seq = separate_groups && i>0 ? i-1 : i;
    specs[i] = CreateActorTaskSpec(seq, false, i==0);
    specs[i].GetMutableMessage().set_task_id(
        TaskID::FromRandom(JobID::FromInt(1)).Binary());
    specs[i].GetMutableMessage().set_name(std::to_string(i));
    specs[i].GetMutableMessage().set_concurrency_group_name(
        separate_groups ? (i==0 ? "g" : "h") : "");
  }
  auto enqueue = [&](int i) {
    const auto &spec = specs[i];
    auto deps = spec.GetDependencies();
    if (!deps.empty()) waiter.BeginArgsFetch(deps, {spec.TaskId(), spec.AttemptNumber()});
    queue.EnqueueTask(separate_groups && i>0 ? i-1 : i, -1,
        TaskToExecute(spec, std::nullopt, &replies[i],
          [&, i](const Status &status, std::function<void()>, std::function<void()>) {
            EXPECT_TRUE(status.ok());
            { std::lock_guard<std::mutex> lock(mu); ++completed[i]; }
            done[i].set_value();
          }));
  };
  enqueue(0);
  enqueue(1);
  EXPECT_EQ(futures[1].wait_for(std::chrono::seconds(5)), std::future_status::ready);
  enqueue(2);
  EXPECT_EQ(futures[2].wait_for(std::chrono::seconds(5)), std::future_status::ready);
  { std::lock_guard<std::mutex> lock(mu); EXPECT_EQ(executed[0], 0); }
  waiter.MarkReady({specs[0].TaskId(), specs[0].AttemptNumber()});
  EXPECT_EQ(futures[0].wait_for(std::chrono::seconds(5)), std::future_status::ready);
  queue.Stop();
  EXPECT_EQ(executed, (std::array<int, 3>{1,1,1}));
  EXPECT_EQ(canceled, (std::array<int, 3>{0,0,0}));
  EXPECT_EQ(completed, (std::array<int, 3>{1,1,1}));
  EXPECT_EQ(order, (std::vector<int>{1,2,0}));
  for (const auto &spec : specs) EXPECT_FALSE(queue.CancelTaskIfFound(spec.TaskId()));
  std::cout << "QUEUE groups=" << separate_groups
            << " order=B,C,A executed=1,1,1 canceled=0,0,0 completed=1,1,1"
            << std::endl;
}
} // namespace

TEST(Phase3Queue, SameGroupMinusOne) { ProductionMinusOneSchedule(false); }
TEST(Phase3Queue, SeparateGroupsMinusOne) { ProductionMinusOneSchedule(true); }

TEST(Phase3Queue, RetryEarlyReadyAndCancellationAccounting) {
  for (bool cancel : {false, true}) {
    instrumented_io_context io;
    ActorTaskExecutionArgWaiter waiter(
        [](const std::vector<rpc::ObjectReference> &, const TaskID &, int64_t) {});
    FakeTaskEventBuffer events;
    ray::observability::FakeRayEventRecorder recorder;
    auto pools = std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>();
    std::array<int, 2> executed{}, canceled{}, completed{};
    std::array<rpc::PushTaskReply, 2> replies;
    UnorderedActorTaskExecutionQueue queue(
        io, waiter, events, recorder, pools, nullptr, false, 1, {},
        [&](TaskToExecute &task) {
          ++executed[task.AttemptNumber()];
          task.send_reply_callback()(Status::OK(), nullptr, nullptr);
        },
        [&](const TaskToExecute &task, const Status &status) {
          EXPECT_TRUE(status.IsSchedulingCancelled());
          ++canceled[task.AttemptNumber()];
          task.send_reply_callback()(status, nullptr, nullptr);
        });
    auto original = CreateActorTaskSpec(0, false, true);
    original.GetMutableMessage().set_task_id(
        TaskID::FromRandom(JobID::FromInt(1)).Binary());
    for (int attempt=0; attempt<2; ++attempt) {
      TaskSpecification spec(original.GetMessage());
      spec.GetMutableMessage().set_attempt_number(attempt);
      spec.GetMutableMessage().mutable_actor_task_spec()
          ->set_concurrency_group_sequence_number(attempt);
      waiter.BeginArgsFetch(spec.GetDependencies(), {spec.TaskId(), attempt});
      queue.EnqueueTask(attempt, -1,
          TaskToExecute(spec, std::nullopt, &replies[attempt],
            [&, attempt](const Status &, std::function<void()>, std::function<void()>) {
              ++completed[attempt];
            }));
    }
    EXPECT_EQ(original.AttemptNumber(), 0);
    // Real waiter supports the deferred attempt becoming ready before its
    // OnArgsReady callback has been registered.
    waiter.MarkReady({original.TaskId(), 1});
    EXPECT_EQ(completed, (std::array<int, 2>{0,0}));
    if (cancel) { EXPECT_TRUE(queue.CancelTaskIfFound(original.TaskId())); }
    waiter.MarkReady({original.TaskId(), 0});
    io.poll();
    EXPECT_EQ(executed, (cancel ? std::array<int, 2>{0,0} : std::array<int, 2>{1,1}));
    EXPECT_EQ(canceled, (cancel ? std::array<int, 2>{1,1} : std::array<int, 2>{0,0}));
    EXPECT_EQ(completed, (std::array<int, 2>{1,1}));
    EXPECT_FALSE(queue.CancelTaskIfFound(original.TaskId()));
    queue.Stop();
    EXPECT_EQ(completed, (std::array<int, 2>{1,1}));
    std::cout << "RETRY cancel=" << cancel
              << " executed=" << executed[0] << "," << executed[1]
              << " canceled=" << canceled[0] << "," << canceled[1]
              << " completed=1,1 pending=false" << std::endl;
  }
}
}} // namespace ray::core
