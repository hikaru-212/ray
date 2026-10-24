// Portable combined derivative: final valid-CPU fixture plus original isolated
// cancellation control. Historical logs came from the separate original harnesses.
// External deterministic experiment for Ray PR #67315. No production edits.
#include <gtest/gtest.h>
#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "ray/common/runtime_env_manager.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs/actor/gcs_actor_manager.h"
#include "ray/gcs/actor/gcs_actor_scheduler.h"
#include "ray/gcs/fake_gcs_kv_manager.h"
#include "ray/gcs/gcs_function_manager.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

namespace ray::gcs {
namespace {
struct LeaseRecord {
  rpc::RequestWorkerLeaseRequest request;
  rpc::ClientCallback<rpc::RequestWorkerLeaseReply> callback;
  rpc::RequestWorkerLeaseReply reply;
  LeaseID lease;
  ActorID actor;
  NodeID node;
  WorkerID worker = WorkerID::Nil();
  bool granted = false, cancel_processed = false, delivered = false, killed = false;
};

// Models transport boundaries, not scheduler decisions. A grant is immutable once
// produced; cancel of a queued request prevents grant, but cannot revoke a grant.
class TracedRaylet : public rpc::FakeRayletClient {
 public:
  NodeID node;
  std::vector<LeaseRecord> leases;
  std::vector<std::pair<LeaseID, rpc::ClientCallback<rpc::CancelWorkerLeaseReply>>> cancels;
  std::vector<rpc::KillLocalActorRequest> kills;
  std::vector<WorkerID> creation_targets;
  void RequestWorkerLease(rpc::RequestWorkerLeaseRequest &&request,
      const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback) override {
    LeaseSpecification spec(request.lease_spec());
    EXPECT_EQ(spec.CallerNodeId(), node) << "owner must live on the sole alive node";
    EXPECT_TRUE(spec.IsActorCreationTask());
    std::cout << "TRACE SUBMIT lease=" << spec.LeaseId().Hex()
              << " actor=" << spec.ActorId().Hex() << " node=" << node.Hex() << std::endl;
    leases.push_back({std::move(request), callback, {}, spec.LeaseId(), spec.ActorId(), node});
  }
  void CancelWorkerLease(const LeaseID &id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {
    std::cout << "TRACE CANCEL_SUBMIT lease=" << id.Hex() << std::endl;
    cancels.emplace_back(id, callback);
  }
  void Grant(size_t index, const WorkerID &worker) {
    auto &r = leases.at(index);
    ASSERT_FALSE(r.cancel_processed);
    ASSERT_FALSE(r.granted);
    r.worker = worker;
    r.granted = true;
    auto *addr = r.reply.mutable_worker_address();
    addr->set_node_id(r.node.Binary());
    addr->set_worker_id(worker.Binary());
    addr->set_ip_address("127.0.0.1");
    addr->set_port(21000 + index);
    r.reply.set_worker_pid(31000 + index);
    auto *resource = r.reply.add_resource_mapping();
    resource->set_name("CPU");
    auto *instance = resource->add_resource_ids();
    instance->set_index(0);
    instance->set_quantity(4);
    std::cout << "TRACE GRANT lease=" << r.lease.Hex() << " worker=" << worker.Hex()
              << " actor=" << r.actor.Hex() << " node=" << r.node.Hex() << std::endl;
  }
  void ProcessCancel(size_t index) {
    auto [id, callback] = cancels.at(index);
    auto it = std::find_if(leases.begin(), leases.end(), [&](const auto &r) { return r.lease == id; });
    ASSERT_NE(it, leases.end());
    it->cancel_processed = true;
    if (!it->granted) {
      it->reply.set_canceled(true);
      it->reply.set_failure_type(rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED);
    }
    std::cout << "TRACE CANCEL_PROCESS lease=" << id.Hex()
              << " already_granted=" << it->granted << " worker_retained=" << (it->granted && !it->killed)
              << std::endl;
    callback(Status::OK(), rpc::CancelWorkerLeaseReply{});
  }
  void Deliver(size_t index) {
    auto &r = leases.at(index);
    ASSERT_TRUE(r.granted || r.cancel_processed);
    ASSERT_FALSE(r.delivered);
    r.delivered = true;
    auto callback = r.callback;
    auto reply = r.reply;  // Keep original grant evidence for audit.
    std::cout << "TRACE DELIVER lease=" << r.lease.Hex() << " worker=" << r.worker.Hex() << std::endl;
    callback(Status::OK(), std::move(reply));
  }
  void KillLocalActor(const rpc::KillLocalActorRequest &request,
      const rpc::ClientCallback<rpc::KillLocalActorReply> &callback) override {
    kills.push_back(request);
    auto worker = WorkerID::FromBinary(request.worker_id());
    auto intended = ActorID::FromBinary(request.intended_actor_id());
    auto it = std::find_if(leases.begin(), leases.end(), [&](const auto &r) { return r.worker == worker && r.granted; });
    EXPECT_NE(it, leases.end());
    if (it != leases.end()) {
      // An uncreated leased worker has Nil CurrentActorID in CoreWorker.
      const bool creation_sent = std::find(creation_targets.begin(), creation_targets.end(), worker) != creation_targets.end();
      EXPECT_EQ(intended, creation_sent ? it->actor : ActorID::Nil());
      EXPECT_TRUE(request.force_kill());
      it->killed = true;
    }
    std::cout << "TRACE KILL worker=" << worker.Hex() << " intended_actor=" << intended.Hex()
              << " force=" << request.force_kill() << std::endl;
    callback(Status::OK(), rpc::KillLocalActorReply{});
  }
  bool WasKilled(const WorkerID &id) const {
    return std::any_of(kills.begin(), kills.end(), [&](const auto &r) { return r.worker_id() == id.Binary(); });
  }
  size_t Orphans() const {
    return std::count_if(leases.begin(), leases.end(), [&](const auto &r) {
      return r.granted && !r.killed &&
          std::find(creation_targets.begin(), creation_targets.end(), r.worker) == creation_targets.end();
    });
  }
};

class TracedWorker : public rpc::FakeCoreWorkerClient {
 public:
  TracedRaylet &raylet;
  explicit TracedWorker(TracedRaylet &r) : raylet(r) {}
  std::vector<rpc::PushTaskRequest> pushes;
  std::vector<rpc::ClientCallback<rpc::PushTaskReply>> pending;
  void PushNormalTask(std::unique_ptr<rpc::PushTaskRequest> request,
      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    pushes.push_back(*request);
    pending.push_back(callback);
    auto worker = WorkerID::FromBinary(request->intended_worker_id());
    raylet.creation_targets.push_back(worker);
    std::cout << "TRACE CREATE worker=" << worker.Hex()
              << " actor=" << ActorID::FromBinary(request->task_spec().actor_creation_task_spec().actor_id()).Hex()
              << " attempt=" << request->task_spec().attempt_number() << std::endl;
  }
  void FinishAll() {
    auto callbacks = std::move(pending);
    pending.clear();
    for (const auto &callback : callbacks) callback(Status::OK(), rpc::PushTaskReply{});
  }
};

// Only observations: all scheduling/cancellation/lease reply methods stay real.
class ObservedScheduler : public GcsActorScheduler {
 public:
  using GcsActorScheduler::GcsActorScheduler;
  std::vector<std::shared_ptr<GcsActor>> scheduled;
  void Schedule(std::shared_ptr<GcsActor> actor) override {
    scheduled.push_back(actor);
    GcsActorScheduler::Schedule(std::move(actor));
  }
  bool Leasing(const NodeID &n, const ActorID &a) const {
    auto it = node_to_actors_when_leasing_.find(n);
    return it != node_to_actors_when_leasing_.end() && it->second.contains(a);
  }
};

class LeaseIdentityExperiment : public ::testing::Test {
 public:
  instrumented_io_context io;
  Clock clock;
  observability::FakeRayEventRecorder events;
  observability::FakeGauge gauge;
  observability::FakeHistogram histogram;
  std::shared_ptr<TracedRaylet> raylet = std::make_shared<TracedRaylet>();
  std::shared_ptr<TracedWorker> worker = std::make_shared<TracedWorker>(*raylet);
  std::unique_ptr<rpc::RayletClientPool> raylets;
  std::unique_ptr<rpc::CoreWorkerClientPool> workers;
  std::unique_ptr<pubsub::GcsPublisher> publisher;
  std::unique_ptr<pubsub::ObservabilityPublisher> observability;
  std::unique_ptr<GcsTableStorage> storage;
  std::unique_ptr<GcsNodeManager> nodes;
  std::unique_ptr<RuntimeEnvManager> runtime;
  std::unique_ptr<FakeInternalKV> kv;
  std::unique_ptr<GCSFunctionManager> functions;
  std::unique_ptr<GcsActorManager> manager;
  ObservedScheduler *scheduler = nullptr;
  std::shared_ptr<GcsActor> actor;
  std::shared_ptr<rpc::GcsNodeInfo> node;
  int completed = 0, failures = 0;
  rpc::CreateActorReply creation_reply;

  void Drain() { io.restart(); while (io.poll() != 0) {} }
  void SetUp() override {
    RayConfig::instance().initialize(R"({"maximum_gcs_destroyed_actor_cached_count":10})");
    publisher = std::make_unique<pubsub::GcsPublisher>(std::make_unique<pubsub::FakePublisher>());
    observability = std::make_unique<pubsub::ObservabilityPublisher>(std::make_unique<pubsub::FakePublisher>());
    storage = std::make_unique<GcsTableStorage>(std::make_unique<InMemoryStoreClient>());
    node = GenNodeInfo();
    raylet->node = NodeID::FromBinary(node->node_id());
    raylets = std::make_unique<rpc::RayletClientPool>([this](const rpc::Address &addr) {
      EXPECT_EQ(addr.node_id(), node->node_id());
      return raylet;
    });
    workers = std::make_unique<rpc::CoreWorkerClientPool>([this](const rpc::Address &) { return worker; });
    nodes = std::make_unique<GcsNodeManager>(publisher.get(), storage.get(), io,
        raylets.get(), ClusterID::Nil(), events, "phase2", observability.get(), clock);
    nodes->AddNode(node);
    ASSERT_EQ(nodes->GetAllAliveNodes().size(), 1u);
    runtime = std::make_unique<RuntimeEnvManager>([](auto, auto done) { done(true); });
    kv = std::make_unique<FakeInternalKV>();
    functions = std::make_unique<GCSFunctionManager>(*kv, io);
    auto sched = std::make_unique<ObservedScheduler>(io, storage->ActorTable(), *nodes,
        [this](auto a, auto type, auto message) { ++failures; manager->OnActorSchedulingFailed(a, type, message); },
        [this](auto a, const rpc::PushTaskReply &reply) { manager->OnActorCreationSuccess(a, reply); },
        *raylets, *workers, histogram, clock);
    scheduler = sched.get();
    manager = std::make_unique<GcsActorManager>(std::move(sched), storage.get(), io,
        publisher.get(), *runtime, *functions, [](const ActorID &) {}, *raylets, *workers,
        events, "phase2", gauge, gauge, observability.get(), clock);
    auto registration = GenRegisterActorRequest(JobID::FromInt(1), 1, false, "", "phase2");
    (*registration.mutable_task_spec()->mutable_required_resources())["CPU"] = 4;
    (*registration.mutable_task_spec()->mutable_required_placement_resources())["CPU"] = 4;
    registration.mutable_task_spec()->mutable_caller_address()->set_node_id(node->node_id());
    registration.mutable_task_spec()->mutable_caller_address()->set_ip_address("127.0.0.1");
    bool registered = false;
    rpc::RegisterActorReply registration_reply;
    manager->HandleRegisterActor(registration, &registration_reply, [&](Status s, auto, auto) {
      EXPECT_TRUE(s.ok()); registered = true;
    });
    Drain();
    ASSERT_TRUE(registered);
    rpc::CreateActorRequest creation;
    creation.mutable_task_spec()->CopyFrom(registration.task_spec());
    manager->HandleCreateActor(creation, &creation_reply, [this](Status s, auto, auto) {
      EXPECT_TRUE(s.ok()); ++completed;
      std::cout << "TRACE COMPLETED worker="
                << WorkerID::FromBinary(creation_reply.actor_address().worker_id()).Hex() << std::endl;
    });
    ASSERT_EQ(scheduler->scheduled.size(), 1u);
    actor = scheduler->scheduled.front();
    ASSERT_EQ(actor->GetState(), rpc::ActorTableData::PENDING_CREATION);
    ASSERT_TRUE(actor->GetWorkerID().IsNil());
    ASSERT_EQ(raylet->leases.size(), 1u);
    ASSERT_EQ(raylet->leases[0].actor, actor->GetActorID());
    ASSERT_TRUE(scheduler->Leasing(raylet->node, actor->GetActorID()));
  }
  void KillAndRestart() {
    rpc::KillActorViaGcsRequest request;
    request.set_actor_id(actor->GetActorID().Binary());
    request.set_force_kill(true);
    request.set_no_restart(false);
    rpc::KillActorViaGcsReply reply;
    bool replied = false;
    manager->HandleKillActorViaGcs(request, &reply, [&](Status s, auto, auto) {
      EXPECT_TRUE(s.ok()); replied = true;
    });
    ASSERT_TRUE(replied);
    ASSERT_EQ(raylet->cancels.size(), 1u);
    ASSERT_EQ(raylet->cancels[0].first, raylet->leases[0].lease);
    ASSERT_EQ(scheduler->scheduled.size(), 2u);
    ASSERT_EQ(scheduler->scheduled[0].get(), scheduler->scheduled[1].get());
    ASSERT_EQ(raylet->leases.size(), 2u);
    ASSERT_NE(raylet->leases[0].lease, raylet->leases[1].lease);
    ASSERT_EQ(raylet->leases[0].actor, raylet->leases[1].actor);
    ASSERT_EQ(raylet->leases[0].node, raylet->leases[1].node);
    ASSERT_EQ(actor->GetLeaseSpecification().LeaseId(), raylet->leases[1].lease);
    ASSERT_TRUE(actor->GetWorkerID().IsNil());
    ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);
    ASSERT_EQ(actor->GetActorTableData().num_restarts(), 1u);
    ASSERT_EQ(actor->GetCreationTaskSpecification().GetMessage().attempt_number(), 1);
    ASSERT_TRUE(scheduler->Leasing(raylet->node, actor->GetActorID()));
    ASSERT_TRUE(worker->pushes.empty());
  }
  void Snapshot(const std::string &stage) {
    std::cout << "SNAPSHOT " << stage << " actor_worker=" << actor->GetWorkerID().Hex()
              << " actor_lease=" << actor->GetLeaseSpecification().LeaseId().Hex()
              << " membership=" << scheduler->Leasing(raylet->node, actor->GetActorID())
              << " pushes=" << worker->pushes.size() << " orphans=" << raylet->Orphans()
              << " completed=" << completed << std::endl;
  }
};


TEST_F(LeaseIdentityExperiment, CompatibleGrantUsesCurrentAttemptAndOwnResources) {
  const auto w1 = WorkerID::FromRandom(), w2 = WorkerID::FromRandom();
  raylet->Grant(0, w1);
  KillAndRestart();
  ASSERT_FALSE(HasFatalFailure());
  auto spec1 = raylet->leases[0].request.lease_spec();
  auto spec2 = raylet->leases[1].request.lease_spec();
  spec1.clear_lease_id(); spec2.clear_lease_id();
  EXPECT_EQ(spec1.SerializeAsString(), spec2.SerializeAsString());
  raylet->ProcessCancel(0);
  raylet->Deliver(0); Drain();
  ASSERT_EQ(worker->pushes.size(), 1u);
  const auto &push = worker->pushes[0];
  EXPECT_EQ(push.intended_worker_id(), w1.Binary());
  EXPECT_EQ(push.task_spec().attempt_number(), 1);
  EXPECT_EQ(push.task_spec().actor_creation_task_spec().actor_id(), actor->GetActorID().Binary());
  ASSERT_EQ(push.resource_mapping_size(), 1);
  EXPECT_EQ(push.resource_mapping(0).name(), "CPU");
  EXPECT_EQ(push.resource_mapping(0).resource_ids(0).index(), 0);
  EXPECT_EQ(push.resource_mapping(0).resource_ids(0).quantity(), 4);
  ASSERT_EQ(actor->GetActorTableData().resource_mapping_size(), 1);
  EXPECT_EQ(actor->GetActorTableData().resource_mapping(0).SerializeAsString(), push.resource_mapping(0).SerializeAsString());
  raylet->Grant(1, w2); raylet->Deliver(1); Drain();
  EXPECT_EQ(worker->pushes.size(), 1u);
  EXPECT_FALSE(raylet->WasKilled(w1));
  EXPECT_TRUE(raylet->WasKilled(w2));
  if (!raylet->kills.empty()) {
    EXPECT_EQ(raylet->kills[0].worker_id(), w2.Binary());
    EXPECT_EQ(raylet->kills[0].intended_actor_id(), ActorID::Nil().Binary());
  }
  EXPECT_EQ(actor->GetActorTableData().resource_mapping_size(), 1);
  EXPECT_EQ(actor->GetActorTableData().resource_mapping(0).resource_ids(0).index(), 0);
  worker->FinishAll(); Drain();
  EXPECT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  EXPECT_EQ(completed, 1); EXPECT_EQ(failures, 0);
  std::cout << "RESOURCE_TRACE current_attempt=1 adopted=W1 cpu_instance=0 quantity=4 creation_count="
            << worker->pushes.size() << " cleanup_W2=" << raylet->WasKilled(w2) << std::endl;
}
TEST_F(LeaseIdentityExperiment, ObserveUnusedReplacementLeaseAtQuiescence) {
  const auto w1 = WorkerID::FromRandom();
  raylet->Grant(0, w1); KillAndRestart();
  ASSERT_FALSE(HasFatalFailure());
  raylet->ProcessCancel(0); raylet->Deliver(0); Drain();
  worker->FinishAll(); Drain();
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetWorkerID(), w1);
  ASSERT_EQ(raylet->leases.size(), 2u);
  EXPECT_FALSE(raylet->leases[1].granted);
  EXPECT_FALSE(raylet->leases[1].delivered);
  EXPECT_FALSE(scheduler->Leasing(raylet->node, actor->GetActorID()));
  EXPECT_EQ(raylet->cancels.size(), 1u);
  EXPECT_EQ(raylet->cancels[0].first, raylet->leases[0].lease);
  std::cout << "PENDING_TRACE alive_on=W1 pending_L2=true GCS_membership=false cancellation_L2=false" << std::endl;
}
// Author's original isolated cancellation scenario: intentionally no L2.
// This is a scheduler cleanup control, not a claim about real RestartActor timing.
TEST_F(LeaseIdentityExperiment, AuthorCleanupWithoutRescheduling) {
  const auto w1 = WorkerID::FromRandom();
  raylet->Grant(0, w1);
  scheduler->CancelOnLeasing(raylet->node, actor->GetActorID(), raylet->leases[0].lease);
  actor->UpdateState(rpc::ActorTableData::RESTARTING);
  raylet->ProcessCancel(0);
  raylet->Deliver(0);
  Drain();
  Snapshot("author_cleanup");
  EXPECT_TRUE(raylet->WasKilled(w1));
  EXPECT_TRUE(actor->GetWorkerID().IsNil());
  EXPECT_TRUE(worker->pushes.empty());
  EXPECT_EQ(raylet->Orphans(), 0u);
  if (!raylet->kills.empty()) {
    EXPECT_EQ(raylet->kills.front().worker_id(), w1.Binary());
    EXPECT_EQ(raylet->kills.front().intended_actor_id(), ActorID::Nil().Binary());
  }
}


} // namespace
} // namespace ray::gcs
