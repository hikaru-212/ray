// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/core_worker.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>
#include "ray/pubsub/fake_publisher.h"
#include "ray/core_worker_rpc_client/core_worker_client.h"
#include "ray/core_worker/task_submission/dependency_resolver.h"
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "ray/asio/fake_periodical_runner.h"
#include "ray/common/buffer.h"
#include "ray/common/ray_config.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/actor_management/actor_creator.h"
#include "ray/core_worker/actor_management/actor_manager.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker_rpc_proxy.h"
#include "ray/core_worker/future_resolver.h"
#include "ray/core_worker/generator_waiter.h"
#include "ray/core_worker/grpc_service.h"
#include "ray/core_worker/object_recovery_manager.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/core_worker/reference_counter_interface.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/task_submission/actor_task_submitter.h"
#include "ray/core_worker/task_submission/normal_task_submitter.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs_rpc_client/fake_gcs_client.h"
#include "ray/object_manager/plasma/fake_plasma_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/pubsub/fake_subscriber.h"
#include "ray/pubsub/publisher.h"
#include "ray/raylet_ipc_client/fake_raylet_ipc_client.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/util/clock.h"

namespace ray {
namespace core {

// Temporary Phase 2 transport seams. Production sources are unchanged.
class Phase2Subscriber : public pubsub::FakeSubscriber {
 public:
  struct Subscription {
    rpc::SubMessage message;
    rpc::Address publisher;
    std::string key;
    pubsub::SubscriptionItemCallback callback;
  };
  std::vector<Subscription> subscriptions;
  void Subscribe(std::unique_ptr<rpc::SubMessage> sub_message,
                 rpc::ChannelType channel_type,
                 const rpc::Address &publisher,
                 const std::optional<std::string> &key,
                 pubsub::SubscribeDoneCallback done,
                 pubsub::SubscriptionItemCallback callback,
                 pubsub::SubscriptionFailureCallback failure) override {
    RAY_CHECK(channel_type == rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL);
    subscriptions.push_back({*sub_message, publisher, *key, std::move(callback)});
  }
  void Unsubscribe(rpc::ChannelType, const rpc::Address &publisher,
                   const std::optional<std::string> &key) override {
    subscriptions.erase(std::remove_if(subscriptions.begin(), subscriptions.end(),
        [&](const Subscription &s) {
          return s.publisher.worker_id() == publisher.worker_id() && s.key == *key;
        }), subscriptions.end());
  }
};

class Phase2Proxy : public CoreWorkerServiceHandlerProxy {
 public:
  CoreWorker *worker = nullptr;
  bool delay = true;
  int handler_calls = 0;
  int replies = 0;
  struct Held {
    rpc::GetObjectStatusRequest request;
    rpc::GetObjectStatusReply *reply;
    rpc::SendReplyCallback send;
  };
  std::vector<Held> held;
  struct PendingReply { bool done = false; rpc::SendReplyCallback send; };
  std::vector<std::shared_ptr<PendingReply>> pending_replies;
  void AbortForHarness() {
    for (const auto &p : pending_replies) {
      if (!p->done) {
        p->done = true;
        p->send(Status::Invalid("explicit test-only teardown"), nullptr, nullptr);
      }
    }
  }
  void HandleGetObjectStatus(rpc::GetObjectStatusRequest request,
                             rpc::GetObjectStatusReply *reply,
                             rpc::SendReplyCallback send) override {
    if (delay) {
      held.push_back({std::move(request), reply, std::move(send)});
    } else {
      Invoke({std::move(request), reply, std::move(send)});
    }
  }
  void Invoke(Held h) {
    handler_calls++;
    auto state = std::make_shared<PendingReply>();
    state->send = std::move(h.send);
    pending_replies.push_back(state);
    worker->HandleGetObjectStatus(std::move(h.request), h.reply,
      [this, state](Status s, std::function<void()> ok,
                    std::function<void()> fail) mutable {
        state->done = true;
        replies++;
        state->send(s, std::move(ok), std::move(fail));
      });
  }
  void Deliver() {
    auto messages = std::move(held);
    held.clear();
    for (auto &h : messages) Invoke(std::move(h));
  }
};

class Phase2PeriodicalRunner : public FakePeriodicalRunner {
 public:
  std::unordered_map<std::string, std::function<void()>> callbacks;
  void RunFnPeriodically(std::function<void()> fn, uint64_t period_ms,
                         std::string name) override {
    callbacks.emplace(std::move(name), std::move(fn));
  }
};

class Phase2Worker : public ::testing::Test {
 public:
  Phase2Worker()
      : io_work_(io_service_.get_executor()),
        task_execution_service_work_(task_execution_service_.get_executor()),
        object_freed_callback_service_work_(
            object_freed_callback_service_.get_executor()) {
    CoreWorkerOptions options;
    options.worker_type = WorkerType::WORKER;
    options.language = Language::PYTHON;
    options.node_ip_address = "127.0.0.1";
    options.task_execution_callback =
        [](const rpc::Address &caller_address,
           TaskType task_type,
           const std::string task_name,
           const RayFunction &ray_function,
           const std::unordered_map<std::string, double> &required_resources,
           const std::vector<std::shared_ptr<RayObject>> &args,
           const std::vector<rpc::ObjectReference> &arg_refs,
           const std::string &debugger_breakpoint,
           const std::string &serialized_retry_exception_allowlist,
           std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *returns,
           std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_returns,
           std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
           std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes,
           bool *is_retryable_error,
           std::string *actor_repr_name,
           std::string *application_error,
           const std::vector<ConcurrencyGroup> &defined_concurrency_groups,
           const std::string name_of_concurrency_group_to_execute,
           bool is_reattempt,
           bool is_streaming_generator,
           bool retry_exception,
           int64_t generator_backpressure_num_objects,
           int64_t num_objects_per_yield,
           const std::optional<std::string> &tensor_transport) -> Status {
      return Status::OK();
    };

    client_call_manager_ = std::make_unique<rpc::ClientCallManager>(
        io_service_, /*record_stats=*/false, /*local_address=*/"");

    auto core_worker_client_pool =
        std::make_shared<rpc::CoreWorkerClientPool>([this](const rpc::Address &address) {
          return std::make_shared<rpc::CoreWorkerClient>(address, *client_call_manager_, [] {});
        });

    auto raylet_client_pool = std::make_shared<rpc::RayletClientPool>(
        [](const rpc::Address &) { return std::make_shared<rpc::FakeRayletClient>(); });

    fake_gcs_client_ = std::make_shared<gcs::FakeGcsClient>();

    auto fake_local_raylet_rpc_client = std::make_shared<rpc::FakeRayletClient>();
    local_raylet_client_ = fake_local_raylet_rpc_client;

    auto fake_raylet_ipc_client = std::make_shared<ipc::FakeRayletIpcClient>();

    service_handler_ = std::make_unique<Phase2Proxy>();
    auto worker_context = std::make_unique<WorkerContext>(
        WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(1));
    auto core_worker_server =
        std::make_unique<rpc::GrpcServer>(WorkerTypeString(options.worker_type), 0, true);
    core_worker_server->RegisterService(
        std::make_unique<rpc::CoreWorkerGrpcService>(
            io_service_, *service_handler_, /*max_active_rpcs_per_handler_=*/-1),
        false /* token_auth */);
    core_worker_server->Run();

    rpc_address_.set_ip_address(options.node_ip_address);
    rpc_address_.set_port(core_worker_server->GetPort());
    rpc_address_.set_node_id(NodeID::FromRandom().Binary());
    rpc_address_.set_worker_id(worker_context->GetWorkerID().Binary());

    fake_periodical_runner_ = std::make_unique<FakePeriodicalRunner>();

    auto object_info_publisher = std::make_unique<pubsub::FakePublisher>();

    object_info_publisher_ = object_info_publisher.get();

    auto fake_object_info_subscriber = std::make_unique<Phase2Subscriber>();
    subscriber_ = fake_object_info_subscriber.get();

    reference_counter_ = std::make_shared<ReferenceCounter>(
        rpc_address_,
        object_info_publisher.get(),
        fake_object_info_subscriber.get(),
        [](const NodeID &) { return false; },
        [this](const ObjectID &object_id, const absl::flat_hash_set<NodeID> &locations) {
          core_worker_->FreeObjectOnNodesAsync(object_id, locations);
        },
        fake_owned_object_count_gauge_,
        fake_owned_object_size_gauge_,
        true);

    // Mock reference counter as enabled
    memory_store_ = std::make_shared<CoreWorkerMemoryStore>(
        io_service_, clock_, reference_counter_ != nullptr, nullptr);

    auto future_resolver = std::make_unique<FutureResolver>(
        memory_store_,
        reference_counter_,
        [](const ObjectID &object_id,
           const absl::flat_hash_set<NodeID> &locations,
           uint64_t object_size) {},
        core_worker_client_pool,
        rpc_address_);

    auto task_event_buffer = std::make_unique<worker::TaskEventBufferImpl>(
        std::make_unique<gcs::FakeGcsClient>(),
        std::make_unique<rpc::EventAggregatorClientImpl>(0, *client_call_manager_),
        "test_session",
        NodeID::Nil(),
        clock_);

    task_manager_ = std::make_shared<TaskManager>(
        *memory_store_,
        *reference_counter_,
        [](const RayObject &object, const ObjectID &object_id) { return Status::OK(); },
        [](TaskSpecification &spec, uint32_t delay_ms) {},
        [](const TaskSpecification &spec) { return false; },
        [](const JobID &job_id,
           const std::string &type,
           const std::string &error_message,
           double timestamp) { return Status::OK(); },
        RayConfig::instance().max_lineage_bytes(),
        *task_event_buffer,
        fake_ray_event_recorder_,
        [](const ActorID &actor_id) {
          return std::make_shared<rpc::FakeCoreWorkerClient>();
        },
        fake_gcs_client_,
        fake_task_by_state_gauge_,
        fake_total_lineage_bytes_gauge_,
        /*free_actor_object_callback=*/[](const ObjectID &object_id) {},
        /*set_direct_transport_metadata=*/[](const ObjectID &, const std::string &) {},
        /*free_stale_unconsumed_generator_objects_async=*/
        [](const ObjectID &, const absl::flat_hash_set<NodeID> &) {},
        /*clock=*/clock_);

    auto object_recovery_manager = std::make_unique<ObjectRecoveryManager>(
        rpc_address_,
        raylet_client_pool,
        [](const ObjectID &object_id, const ObjectLookupCallback &callback) {
          callback(object_id, {});
          return Status::OK();
        },
        *task_manager_,
        *reference_counter_,
        *memory_store_,
        [](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {});

    auto lease_policy = std::unique_ptr<LeasePolicyInterface>(
        std::make_unique<LocalLeasePolicy>(rpc_address_));

    auto lease_request_rate_limiter = std::make_shared<StaticLeaseRequestRateLimiter>(10);

    actor_creator_ = std::make_shared<ActorCreator>(fake_gcs_client_->Actors());

    auto normal_task_submitter = std::make_unique<NormalTaskSubmitter>(
        rpc_address_,
        fake_local_raylet_rpc_client,
        core_worker_client_pool,
        raylet_client_pool,
        fake_gcs_client_,
        std::move(lease_policy),
        memory_store_,
        *task_manager_,
        NodeID::Nil(),
        WorkerType::WORKER,
        10000,
        actor_creator_,
        JobID::Nil(),
        lease_request_rate_limiter,
        [](const ObjectID &object_id) { return std::nullopt; },
        io_service_,
        fake_scheduler_placement_time_ms_histogram_,
        /*clock=*/clock_);

    auto actor_task_submitter = std::make_unique<ActorTaskSubmitter>(
        *core_worker_client_pool,
        *raylet_client_pool,
        fake_gcs_client_,
        *memory_store_,
        *task_manager_,
        *actor_creator_,
        /*tensor_transport_getter=*/
        [](const ObjectID &object_id) { return std::nullopt; },
        [](const ActorID &actor_id, const std::string &, uint64_t num_queued) {},
        io_service_,
        reference_counter_,
        /*clock=*/clock_);
    actor_task_submitter_ = actor_task_submitter.get();

    auto actor_manager = std::make_unique<ActorManager>(
        fake_gcs_client_, *actor_task_submitter, *reference_counter_);

    auto periodical_runner = std::make_unique<Phase2PeriodicalRunner>();

    // TODO(joshlee): Dependency inject socket into plasma_store_provider_ so we can
    // create a real plasma_store_provider_ and mutable_object_provider_
    core_worker_ = std::make_shared<CoreWorker>(
        std::move(options),
        std::move(worker_context),
        io_service_,
        object_freed_callback_service_,
        std::move(core_worker_client_pool),
        std::move(raylet_client_pool),
        std::move(periodical_runner),
        std::move(core_worker_server),
        std::move(rpc_address_),
        fake_gcs_client_,
        std::move(fake_raylet_ipc_client),
        std::move(fake_local_raylet_rpc_client),
        io_thread_,
        object_freed_callback_thread_,
        reference_counter_,
        memory_store_,
        nullptr,  // plasma_store_provider_
        nullptr,  // mutable_object_provider_
        std::move(future_resolver),
        task_manager_,
        actor_creator_,
        std::move(actor_task_submitter),
        std::move(object_info_publisher),
        std::move(fake_object_info_subscriber),
        std::move(lease_request_rate_limiter),
        std::move(normal_task_submitter),
        std::move(object_recovery_manager),
        std::move(actor_manager),
        task_execution_service_,
        std::move(task_event_buffer),
        std::make_unique<observability::FakeRayEventRecorder>(),
        getpid(),
        fake_task_by_state_gauge_,
        fake_actor_by_state_gauge_,
        clock_);
    service_handler_->worker = core_worker_.get();
    service_handler_->SetCoreWorker(core_worker_.get());
  }

 public:
  void TestBody() override {}
  std::unique_ptr<Phase2Proxy> service_handler_;
  Phase2Subscriber *subscriber_;
  FakeClock clock_;
  instrumented_io_context io_service_;
  instrumented_io_context task_execution_service_;
  instrumented_io_context object_freed_callback_service_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      task_execution_service_work_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      object_freed_callback_service_work_;

  boost::thread io_thread_;
  boost::thread object_freed_callback_thread_;

  /// Flush all pending object-freed callbacks. Call this in tests after an action
  /// that should trigger a user-registered out-of-scope callback.
  void FlushObjectFreedCallbacks() { object_freed_callback_service_.poll(); }

  rpc::Address rpc_address_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::shared_ptr<ReferenceCounterInterface> reference_counter_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;
  ActorTaskSubmitter *actor_task_submitter_;
  std::shared_ptr<rpc::FakeRayletClient> local_raylet_client_;
  pubsub::FakePublisher *object_info_publisher_;
  std::shared_ptr<TaskManager> task_manager_;
  std::shared_ptr<gcs::FakeGcsClient> fake_gcs_client_;
  std::shared_ptr<ActorCreator> actor_creator_;
  std::shared_ptr<CoreWorker> core_worker_;
  ray::observability::FakeRayEventRecorder fake_ray_event_recorder_;
  ray::observability::FakeGauge fake_task_by_state_gauge_;
  ray::observability::FakeGauge fake_actor_by_state_gauge_;
  ray::observability::FakeGauge fake_total_lineage_bytes_gauge_;
  ray::observability::FakeHistogram fake_scheduler_placement_time_ms_histogram_;
  ray::observability::FakeGauge fake_owned_object_count_gauge_;
  ray::observability::FakeGauge fake_owned_object_size_gauge_;
  std::unique_ptr<FakePeriodicalRunner> fake_periodical_runner_;
};

// Only tests use -fno-access-control. Private containers are observed, never edited.
// Production changes to refs, values and waiters occur exclusively through their APIs.
bool HeadExpected() {
  const char *value = std::getenv("PHASE2_EXPECT_DELETE");
  RAY_CHECK(value != nullptr);
  return std::string(value) == "1";
}

ReferenceCounter &Refs(Phase2Worker &w) {
  return *static_cast<ReferenceCounter *>(w.reference_counter_.get());
}
rpc::Address Address(Phase2Worker &w) { return w.core_worker_->rpc_address_; }
size_t Pending(Phase2Worker &w, const ObjectID &x) {
  absl::MutexLock lock(&w.memory_store_->mu_);
  auto it = w.memory_store_->object_async_get_requests_.find(x);
  return it == w.memory_store_->object_async_get_requests_.end() ? 0 : it->second.size();
}
size_t Active(Phase2Worker &b, Phase2Worker &o) {
  auto client = std::static_pointer_cast<rpc::CoreWorkerClient>(
      b.core_worker_->core_worker_client_pool_->GetOrConnect(Address(o)));
  return client->retryable_grpc_client_->NumActiveRequests();
}
void Pump(Phase2Worker &o, Phase2Worker &b) {
  o.io_service_.poll();
  b.io_service_.poll();
}
bool Await(Phase2Worker &o, Phase2Worker &b, const std::function<bool()> &pred,
           int milliseconds = 5000) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(milliseconds);
  do {
    Pump(o, b);
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  } while (std::chrono::steady_clock::now() < deadline);
  return pred();
}
void Observe(const char *stage, Phase2Worker &o, Phase2Worker &b, const ObjectID &x) {
  auto &rc = Refs(o);
  rpc::Address owner;
  const bool owner_present = rc.GetOwner(x, &owner);
  std::cout << "OBS " << stage << " owner=" << owner_present
            << " store=" << (o.memory_store_->GetIfExists(x) != nullptr)
            << " pending=" << Pending(o, x)
            << " handler=" << o.service_handler_->handler_calls
            << " reply=" << o.service_handler_->replies
            << " client_active=" << Active(b, o)
            << " borrower_has_ref=" << Refs(b).HasReference(x);
  {
    absl::MutexLock lock(&rc.mutex_);
    auto it = rc.object_id_refs_.find(x);
    if (it != rc.object_id_refs_.end()) {
      const auto &r = it->second;
      std::cout << " local=" << r.local_ref_count
                << " submitted=" << r.submitted_task_ref_count
                << " borrowers=" << r.borrow().borrowers.size()
                << " lineage=" << r.lineage_ref_count
                << " out_of_scope=" << r.OutOfScope(true)
                << " should_delete=" << r.ShouldDelete(true);
    }
  }
  std::cout << std::endl;
}

TaskSpecification Task(Phase2Worker &owner, const std::vector<ObjectID> &deps = {},
                       bool nested = false) {
  TaskSpecification spec;
  auto &msg = spec.GetMutableMessage();
  msg.set_task_id(TaskID::FromRandom(JobID::FromInt(1)).Binary());
  msg.set_num_returns(1);
  for (const auto &dep : deps) {
    auto *arg = msg.add_args();
    rpc::ObjectReference *ref = nullptr;
    if (nested) {
      arg->set_data("test frontend payload containing ObjectRef");
      ref = arg->add_nested_inlined_refs();
    } else {
      ref = arg->mutable_object_ref();
    }
    ref->set_object_id(dep.Binary());
    ref->mutable_owner_address()->CopyFrom(Address(owner));
  }
  return spec;
}
void Complete(Phase2Worker &owner, const TaskSpecification &spec, bool plasma,
              const rpc::Address &executor,
              const ReferenceCounterInterface::ReferenceTableProto &borrowed = {}) {
  rpc::PushTaskReply reply;
  auto *result = reply.add_return_objects();
  result->set_object_id(spec.ReturnId(0).Binary());
  result->set_size(100000);
  result->set_in_plasma(plasma);
  if (!plasma) result->set_data("completed");
  reply.mutable_borrowed_refs()->CopyFrom(borrowed);
  owner.task_manager_->CompletePendingTask(spec.TaskId(), reply, executor, false);
}

// Models the C++ calls made by ExecuteTask/deserialize/ExecuteTask completion.
// The borrowed-reference proto is produced by the borrower's real ReferenceCounter.
// It is not synthesized by the test.
void BorrowInTask(Phase2Worker &o, Phase2Worker &b, const ObjectID &x,
                  const std::string &serialized_status, bool expect_inline) {
  auto spec = Task(o, {x}, true);
  o.task_manager_->AddPendingTask(Address(o), spec, "phase2 borrower task", 0);
  LocalDependencyResolver resolver(*o.memory_store_, *o.task_manager_,
                                  *o.actor_creator_,
                                  [](const ObjectID &) { return std::nullopt; });
  bool dependencies_ready = false;
  resolver.ResolveDependencies(spec, [&](Status s) {
    RAY_CHECK(s.ok());
    dependencies_ready = true;
  });
  ASSERT_TRUE(dependencies_ready);  // Nested X does not force resolution.
  b.core_worker_->AddLocalReference(x, "task execution sentinel");
  b.core_worker_->AddLocalReference(x, "frontend ObjectRef retained by borrower");
  b.core_worker_->RegisterOwnershipInfoAndResolveFuture(
      x, ObjectID::Nil(), Address(o), serialized_status);
  rpc::GetObjectStatusReply serialized;
  ASSERT_TRUE(serialized.ParseFromString(serialized_status));
  ASSERT_EQ(serialized.has_object(), expect_inline);
  ReferenceCounterInterface::ReferenceTableProto refs;
  std::vector<ObjectID> deleted;
  Refs(b).PopAndClearLocalBorrowers({x}, &refs, &deleted);
  b.memory_store_->Delete(deleted);
  Complete(o, spec, false, Address(b), refs);
  o.core_worker_->RemoveLocalReference(spec.ReturnId(0));
  auto subscriptions = o.subscriber_->subscriptions;
  int deliveries = 0;
  for (const auto &s : subscriptions) {
    if (s.publisher.worker_id() == Address(b).worker_id() && s.key == x.Binary()) {
      b.core_worker_->ProcessSubscribeForRefRemoved(s.message.worker_ref_removed_message());
      deliveries++;
    }
  }
  ASSERT_EQ(deliveries, 1);
}
void ReleaseBorrower(Phase2Worker &o, Phase2Worker &b, const ObjectID &x) {
  b.core_worker_->RemoveLocalReference(x);
  auto publications = std::move(b.object_info_publisher_->publish_calls);
  b.object_info_publisher_->publish_calls.clear();
  int deliveries = 0;
  for (const auto &msg : publications) {
    if (!msg.has_worker_ref_removed_message()) continue;
    auto subscriptions = o.subscriber_->subscriptions;
    for (const auto &s : subscriptions) {
      if (s.publisher.worker_id() == Address(b).worker_id() && s.key == msg.key_id()) {
        s.callback(rpc::PubMessage(msg));
        deliveries++;
      }
    }
  }
  ASSERT_EQ(deliveries, 1);
}

struct Scenario {
  Phase2Worker o;
  Phase2Worker b;
  TaskSpecification f;
  TaskSpecification g;
  ObjectID x;
  ObjectID y;
  rpc::Address executor;
  Scenario() : f(Task(o)), g(), x(f.ReturnId(0)), y(ObjectID::Nil()) {
    executor.set_worker_id(WorkerID::FromRandom().Binary());
    executor.set_node_id(NodeID::FromRandom().Binary());
    executor.set_ip_address("127.0.0.1");
    executor.set_port(12345);
  }
  void Establish() {
    o.task_manager_->AddPendingTask(Address(o), f, "phase2 F", 3);
    rpc::Address owner;
    std::string status;
    ASSERT_TRUE(o.core_worker_->GetOwnershipInfo(x, &owner, &status).ok());
    ASSERT_EQ(owner.worker_id(), Address(o).worker_id());
    BorrowInTask(o, b, x, status, false);
    ASSERT_TRUE(Await(o, b, [&] { return o.service_handler_->held.size() == 1; }));
    ASSERT_EQ(o.service_handler_->handler_calls, 0);
    Complete(o, f, true, executor);
    g = Task(o, {x});
    y = g.ReturnId(0);
    o.task_manager_->AddPendingTask(Address(o), g, "phase2 G(X)", 3);
    Complete(o, g, true, executor);
    ASSERT_TRUE(o.task_manager_->IsTaskSubmissible(f.TaskId()));
    ASSERT_TRUE(o.task_manager_->IsTaskSubmissible(g.TaskId()));
    Observe("before_owner_release", o, b, x);
    o.core_worker_->RemoveLocalReference(x);
    Observe("before_borrower_release", o, b, x);
    ReleaseBorrower(o, b, x);
    Observe("after_borrower_release", o, b, x);
    ASSERT_EQ(o.memory_store_->GetIfExists(x) == nullptr, HeadExpected());
    ASSERT_TRUE(Refs(o).HasReference(x));
    ASSERT_FALSE(Refs(b).HasReference(x));
    {
      auto &r = Refs(o).object_id_refs_.at(x);
      ASSERT_EQ(r.local_ref_count, 0);
      ASSERT_EQ(r.submitted_task_ref_count, 0);
      ASSERT_TRUE(r.borrow().borrowers.empty());
      ASSERT_EQ(r.lineage_ref_count, 1);
      ASSERT_TRUE(r.OutOfScope(true));
      ASSERT_FALSE(r.ShouldDelete(true));
    }
    o.service_handler_->Deliver();
    ASSERT_TRUE(Await(o, b, [&] {
      return HeadExpected() ? Pending(o, x) == 1 : Active(b, o) == 0;
    }));
    Observe("after_delayed_handler", o, b, x);
    ASSERT_EQ(Pending(o, x), HeadExpected() ? 1 : 0);
    ASSERT_EQ(o.service_handler_->replies, HeadExpected() ? 0 : 1);
    ASSERT_EQ(Active(b, o), HeadExpected() ? 1 : 0);
  }
  // Explicit test-only cleanup, never counted as a production completion path.
  void AbortForHarness() {
    std::vector<CoreWorkerMemoryStore::AsyncGetCallbackId> ids;
    {
      absl::MutexLock lock(&o.memory_store_->mu_);
      auto it = o.memory_store_->object_async_get_requests_.find(x);
      if (it != o.memory_store_->object_async_get_requests_.end()) {
        for (const auto &r : it->second) ids.push_back(r.id);
      }
    }
    for (auto id : ids) o.memory_store_->CancelGetAsync(x, id);
    o.service_handler_->AbortForHarness();
    ASSERT_TRUE(Await(o, b, [&] { return Active(b, o) == 0; }));
  }
};

TEST(Phase2, A1DelayedStatusAndFinalLineageRelease) {
  Scenario s;
  s.Establish();
  ASSERT_FALSE(::testing::Test::HasFatalFailure());
  // B has no ObjectRef, but the actual retryable client still has an active RPC.
  // Polling an idle owner cannot produce the missing value.
  Await(s.o, s.b, [] { return false; }, 100);
  Observe("after_idle_poll", s.o, s.b, s.x);
  s.o.core_worker_->RemoveLocalReference(s.y);
  ASSERT_FALSE(Refs(s.o).HasReference(s.x));
  Observe("after_final_lineage_release", s.o, s.b, s.x);
  ASSERT_EQ(Pending(s.o, s.x), HeadExpected() ? 1 : 0);
  ASSERT_EQ(Active(s.b, s.o), HeadExpected() ? 1 : 0);
  // A fresh request after metadata removal returns OUT_OF_SCOPE immediately.
  rpc::GetObjectStatusRequest request;
  request.set_object_id(s.x.Binary());
  request.set_owner_worker_id(Address(s.o).worker_id());
  rpc::GetObjectStatusReply reply;
  bool replied = false;
  s.o.core_worker_->HandleGetObjectStatus(request, &reply,
      [&](Status status, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(status.ok()); replied = true;
      });
  ASSERT_TRUE(replied);
  ASSERT_EQ(reply.status(), rpc::GetObjectStatusReply::OUT_OF_SCOPE);
  s.AbortForHarness();
}

TEST(Phase2, A2TwoBorrowersActualMemoryStore) {
  Phase2Worker o, b1, b2;
  auto f = Task(o);
  const auto x = f.ReturnId(0);
  o.task_manager_->AddPendingTask(Address(o), f, "phase2 nonretryable producer", 0);
  Complete(o, f, true, Address(b1));
  rpc::Address owner;
  std::string status;
  ASSERT_TRUE(o.core_worker_->GetOwnershipInfo(x, &owner, &status).ok());
  BorrowInTask(o, b1, x, status, true);
  BorrowInTask(o, b2, x, status, true);
  ASSERT_EQ(Refs(o).object_id_refs_.at(x).borrow().borrowers.size(), 2);
  o.core_worker_->RemoveLocalReference(x);
  Observe("A2_two_borrowers", o, b1, x);
  ReleaseBorrower(o, b1, x);
  ASSERT_NE(o.memory_store_->GetIfExists(x), nullptr);
  ASSERT_EQ(Refs(o).object_id_refs_.at(x).borrow().borrowers.size(), 1);
  Observe("A2_one_borrower", o, b2, x);
  ReleaseBorrower(o, b2, x);
  Observe("A2_last_borrower", o, b2, x);
  ASSERT_FALSE(Refs(o).HasReference(x));
  ASSERT_EQ(o.memory_store_->GetIfExists(x) == nullptr, HeadExpected());
}

TEST(Phase2, A3ExplicitGrpcCancellationDoesNotCancelServerWaiter) {
  Scenario s;
  s.Establish();
  ASSERT_FALSE(::testing::Test::HasFatalFailure());
  if (!HeadExpected()) {
    s.o.core_worker_->RemoveLocalReference(s.y);
    return;
  }
  // This extra cancellation control uses raw gRPC because production FutureResolver
  // does not expose a cancellation handle. Its state is not used to create A1.
  s.o.service_handler_->delay = false;
  auto channel = grpc::CreateChannel(
      Address(s.o).ip_address() + ":" + std::to_string(Address(s.o).port()),
      grpc::InsecureChannelCredentials());
  auto stub = rpc::CoreWorkerService::NewStub(channel);
  grpc::ClientContext context;
  grpc::CompletionQueue cq;
  grpc::Status status;
  rpc::GetObjectStatusRequest request;
  request.set_object_id(s.x.Binary());
  request.set_owner_worker_id(Address(s.o).worker_id());
  rpc::GetObjectStatusReply reply;
  auto call = stub->AsyncGetObjectStatus(&context, request, &cq);
  call->Finish(&reply, &status, reinterpret_cast<void *>(1));
  ASSERT_TRUE(Await(s.o, s.b, [&] { return Pending(s.o, s.x) == 2; }));
  context.TryCancel();
  void *tag = nullptr;
  bool ok = false;
  ASSERT_EQ(cq.AsyncNext(&tag, &ok, std::chrono::system_clock::now() +
                                    std::chrono::seconds(5)),
            grpc::CompletionQueue::GOT_EVENT);
  ASSERT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
  Pump(s.o, s.b);
  ASSERT_EQ(Pending(s.o, s.x), 2);
  Observe("A3_after_raw_client_cancel", s.o, s.b, s.x);
  s.o.core_worker_->RemoveLocalReference(s.y);
  ASSERT_EQ(Pending(s.o, s.x), 2);
  Observe("A3_after_lineage_removed", s.o, s.b, s.x);
  s.AbortForHarness();
  cq.Shutdown();
}

TEST(Phase2, A3LegitimateNodeLossReconstructionCompletesStatus) {
  Scenario s;
  s.Establish();
  ASSERT_FALSE(::testing::Test::HasFatalFailure());
  // Actual node-loss ReferenceCounter entry point and actual CoreWorker recovery
  // timer callback. Fake directory reports that no secondary copy survived.
  Refs(s.o).ResetObjectsOnRemovedNode(NodeID::FromBinary(s.executor.node_id()));
  auto *runner = static_cast<Phase2PeriodicalRunner *>(
      s.o.core_worker_->periodical_runner_.get());
  runner->callbacks.at("CoreWorker.RecoverObjects")();
  ASSERT_TRUE(s.o.task_manager_->IsTaskPending(s.f.TaskId()));
  ASSERT_TRUE(s.o.task_manager_->IsTaskPending(s.g.TaskId()));
  Observe("A3_reconstruction_started", s.o, s.b, s.x);
  // Task execution itself remains the existing TaskManager test seam.
  auto reconstructed_executor = s.executor;
  reconstructed_executor.set_node_id(NodeID::FromRandom().Binary());
  Complete(s.o, s.f, true, reconstructed_executor);
  Complete(s.o, s.g, true, reconstructed_executor);
  ASSERT_EQ(s.o.memory_store_->GetIfExists(s.x), nullptr);
  Observe("A3_value_deleted_before_posted_callbacks", s.o, s.b, s.x);
  ASSERT_TRUE(Await(s.o, s.b, [&] { return Active(s.b, s.o) == 0; }));
  Observe("A3_reconstruction_completed", s.o, s.b, s.x);
  ASSERT_EQ(Pending(s.o, s.x), 0);
  ASSERT_EQ(s.o.service_handler_->replies, 1);
  s.o.core_worker_->RemoveLocalReference(s.y);
}

void BorrowReturnedPut(Phase2Worker &o, Phase2Worker &b, const ObjectID &x) {
  auto call = Task(b);
  b.task_manager_->AddPendingTask(Address(b), call, "return nested put ref", 0);
  const auto outer = call.ReturnId(0);
  int64_t inlined_bytes = 0;
  std::shared_ptr<RayObject> value;
  ASSERT_TRUE(o.core_worker_->AllocateReturnObject(
      outer, 1, nullptr, {x}, Address(b), &inlined_bytes, &value).ok());
  ASSERT_NE(value, nullptr);
  ASSERT_EQ(value->GetNestedRefs().size(), 1);
  rpc::PushTaskReply reply;
  auto *result = reply.add_return_objects();
  result->set_object_id(outer.Binary());
  result->set_data("frontend payload containing X");
  for (const auto &ref : value->GetNestedRefs()) {
    result->add_nested_inlined_refs()->CopyFrom(ref);
  }
  b.task_manager_->CompletePendingTask(call.TaskId(), reply, Address(o), false);
  rpc::Address owner;
  std::string status;
  ASSERT_TRUE(o.core_worker_->GetOwnershipInfo(x, &owner, &status).ok());
  b.core_worker_->AddLocalReference(x, "deserialized nested put ref");
  b.core_worker_->RegisterOwnershipInfoAndResolveFuture(x, outer, owner, status);
  auto subscriptions = o.subscriber_->subscriptions;
  int deliveries = 0;
  for (const auto &sub : subscriptions) {
    if (sub.publisher.worker_id() == Address(b).worker_id() && sub.key == x.Binary()) {
      b.core_worker_->ProcessSubscribeForRefRemoved(sub.message.worker_ref_removed_message());
      deliveries++;
    }
  }
  ASSERT_EQ(deliveries, 1);
  b.core_worker_->RemoveLocalReference(outer);
  ASSERT_TRUE(Refs(b).HasReference(x));
}

TEST(Phase2, A2OriginalPutNestedReturnTwoBorrowers) {
  Phase2Worker o, b1, b2;
  // Put/seal boundary: the same owned metadata and real in-process plasma marker
  // as CoreWorker::Put/SealExisting. No physical plasma daemon is simulated.
  const auto x = ObjectID::FromIndex(TaskID::FromRandom(JobID::FromInt(1)), 2);
  Refs(o).AddOwnedObject(x, {}, Address(o), "put/seal boundary", 100000,
      LineageReconstructionEligibility::INELIGIBLE_PUT, true,
      NodeID::FromBinary(Address(o).node_id()));
  o.memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
      x, Refs(o).HasReference(x));
  ASSERT_NE(o.memory_store_->GetIfExists(x), nullptr);
  BorrowReturnedPut(o, b1, x);
  BorrowReturnedPut(o, b2, x);
  ASSERT_EQ(Refs(o).object_id_refs_.at(x).borrow().borrowers.size(), 2);
  o.core_worker_->RemoveLocalReference(x);
  Observe("A2_put_two_borrowers", o, b1, x);
  ReleaseBorrower(o, b1, x);
  ASSERT_NE(o.memory_store_->GetIfExists(x), nullptr);
  ASSERT_EQ(Refs(o).object_id_refs_.at(x).borrow().borrowers.size(), 1);
  Observe("A2_put_one_borrower", o, b2, x);
  ReleaseBorrower(o, b2, x);
  Observe("A2_put_last_borrower", o, b2, x);
  ASSERT_FALSE(Refs(o).HasReference(x));
  ASSERT_EQ(o.memory_store_->GetIfExists(x) == nullptr, HeadExpected());
}

}  // namespace core
}  // namespace ray
