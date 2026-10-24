#include <future>
#include <iostream>
#include <mutex>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "gtest/gtest.h"
#include "ray/core_worker/task_submission/out_of_order_actor_submit_queue.h"
#include "ray/core_worker_rpc_client/core_worker_client.h"

namespace ray {
namespace {
class RecordingService : public rpc::CoreWorkerService::Service {
 public:
  grpc::Status PushTask(grpc::ServerContext *, const rpc::PushTaskRequest *request,
                        rpc::PushTaskReply *) override {
    std::lock_guard<std::mutex> lock(mu);
    records.push_back(*request);
    const bool fail = request->task_spec().name() == "A" &&
                      request->task_spec().attempt_number() == 0;
    return fail ? grpc::Status(grpc::StatusCode::UNAVAILABLE, "controlled failure")
                : grpc::Status::OK;
  }
  std::mutex mu;
  std::vector<rpc::PushTaskRequest> records;
};

TEST(Phase3Wire, ActualQueueToRealClient) {
  RecordingService service;
  grpc::ServerBuilder builder;
  int port = 0;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ASSERT_GT(port, 0);
  instrumented_io_context io;
  auto work = boost::asio::make_work_guard(io);
  rpc::ClientCallManager calls(io, false, "");
  std::thread runner([&] { io.run(); });
  rpc::Address address;
  address.set_ip_address("127.0.0.1");
  address.set_port(port);
  address.set_worker_id(WorkerID::FromRandom().Binary());
  auto client = std::make_shared<rpc::CoreWorkerClient>(address, calls, [] {});
  core::OutofOrderActorSubmitQueue queue;
  const auto job = JobID::FromInt(1);
  auto make = [&](std::string name, std::string group, int64_t seq) {
    TaskSpecification spec;
    auto &m = spec.GetMutableMessage();
    m.set_type(rpc::TaskType::ACTOR_TASK);
    m.set_task_id(TaskID::FromRandom(job).Binary());
    m.set_job_id(job.Binary());
    m.set_name(name);
    m.set_concurrency_group_name(group);
    m.mutable_actor_task_spec()->set_concurrency_group_sequence_number(seq);
    return spec;
  };
  auto a = make("A", "g", 0);
  a.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      ObjectID::FromRandom().Binary());
  EXPECT_EQ(a.NumArgs(), 1);
  auto b = make("B", "g", 1);
  queue.Emplace("g", 0, a);
  queue.Emplace("g", 1, b);
  std::vector<bool> observed_skip;
  auto send = [&](std::unique_ptr<rpc::PushTaskRequest> request, bool skip) {
    auto promise = std::make_shared<std::promise<Status>>();
    auto future = promise->get_future();
    client->PushActorTask(std::move(request), skip,
        [promise](const Status &status, rpc::PushTaskReply &&) {
          promise->set_value(status);
        });
    if (future.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
      ADD_FAILURE() << "RPC completion deadline exceeded";
      return false;
    }
    return future.get().ok();
  };
  auto ready_send = [&](const std::string &group, int64_t seq, bool expect_ok) {
    queue.MarkDependencyResolved(group, seq);
    auto next = queue.PopNextTaskToSend();
    EXPECT_TRUE(next.has_value());
    if (!next) return;
    observed_skip.push_back(next->second);
    auto request = std::make_unique<rpc::PushTaskRequest>();
    request->mutable_task_spec()->CopyFrom(next->first.GetMessage());
    request->set_sequence_number(next->first.ConcurrencyGroupSequenceNumber());
    request->set_intended_worker_id(address.worker_id());
    // Deliberately do NOT assign client_processed_up_to.
    EXPECT_EQ(send(std::move(request), next->second), expect_ok);
  };
  ready_send("g", 1, true);  // B reply is observed before C is submitted.
  auto c = make("C", "g", 2);
  queue.Emplace("g", 2, c);
  ready_send("g", 2, true);
  ready_send("g", 0, false);
  // Retry shape follows CoreWorker's same-ID/new-attempt/new-sequence contract.
  // The automatic retry policy itself is outside this harness.
  TaskSpecification retry(a.GetMessage());
  retry.GetMutableMessage().set_attempt_number(1);
  retry.GetMutableMessage().mutable_actor_task_spec()
      ->set_concurrency_group_sequence_number(3);
  queue.Emplace("g", 3, retry);
  ready_send("g", 3, true);
  auto other = make("D", "h", 0);
  queue.Emplace("h", 0, other);
  ready_send("h", 0, true);
  client = std::make_shared<rpc::CoreWorkerClient>(address, calls, [] {});
  auto reconnect = make("E", "g", 4);
  queue.Emplace("g", 4, reconnect);
  ready_send("g", 4, true);
  EXPECT_TRUE(queue.Empty());
  EXPECT_EQ(observed_skip, std::vector<bool>(6, true));

  // Positive control for the observation seam: direct ordered client branch,
  // not a request routed to an unordered receiver.
  for (int64_t seq : {0, 1}) {
    auto spec = make("ordered-control", "ordered-control", seq);
    auto request = std::make_unique<rpc::PushTaskRequest>();
    request->mutable_task_spec()->CopyFrom(spec.GetMessage());
    request->set_sequence_number(seq);
    EXPECT_TRUE(send(std::move(request), false));
  }
  client.reset();
  work.reset();
  io.stop();
  runner.join();
  server->Shutdown();
  server->Wait();
  {
    std::lock_guard<std::mutex> lock(service.mu);
    ASSERT_EQ(service.records.size(), 8);
    std::vector<int64_t> expected{1, 2, 0, 3, 0, 4, 0, 1};
    for (size_t i = 0; i < service.records.size(); ++i) {
      const auto &r = service.records[i];
      std::cout << "WIRE index=" << i << " seq=" << r.sequence_number()
                << " watermark=" << r.client_processed_up_to()
                << " group=" << r.task_spec().concurrency_group_name()
                << " attempt=" << r.task_spec().attempt_number()
                << " skip=" << (i < 6 ? observed_skip[i] : false) << std::endl;
      EXPECT_EQ(r.sequence_number(), expected[i]);
      EXPECT_EQ(r.client_processed_up_to(), i == 7 ? 0 : -1);
      EXPECT_EQ(r.task_spec().concurrency_group_name(),
                i >= 6 ? "ordered-control" : (i == 4 ? "h" : "g"));
    }
    EXPECT_EQ(service.records[2].task_spec().task_id(),
              service.records[3].task_spec().task_id());
    EXPECT_EQ(service.records[3].task_spec().attempt_number(), 1);
  }
}
}  // namespace
}  // namespace ray
