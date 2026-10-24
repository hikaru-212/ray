// Copyright 2017 The Ray Authors.
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

// clang-format off
#include "ray/raylet/scheduling/cluster_lease_manager.h"

#include <memory>
#include <string>
#include <list>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "ray/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/label_selector.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/lease/lease.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_utils.h"
#include "ray/gcs_rpc_client/fake_gcs_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/raylet/scheduling/local_lease_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/tests/util.h"
#include "ray/util/clock.h"
// clang-format on

namespace ray {

namespace raylet {

class FakeWorkerPool : public WorkerPoolInterface {
 public:
  FakeWorkerPool() : num_pops(0) {}

  void PopWorker(const LeaseSpecification &lease_spec,
                 const PopWorkerCallback &callback) override {
    num_pops++;
    const int runtime_env_hash = lease_spec.GetRuntimeEnvHash();
    callbacks[runtime_env_hash].push_back(callback);
  }

  void PushWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    workers.push_front(worker);
  }

  std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredWorkers(
      bool filter_dead_workers, bool filter_io_workers) const override {
    RAY_CHECK(false) << "Not used.";
    return {};
  }

  bool AllAliveWorkersAreActors() const override {
    RAY_CHECK(false) << "Not used.";
    return false;
  }

  std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const WorkerID &worker_id) const override {
    RAY_CHECK(false) << "Not used.";
    return nullptr;
  };

  std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const WorkerID &worker_id) const override {
    RAY_CHECK(false) << "Not used.";
    return nullptr;
  }

  void TriggerCallbacksWithNotOKStatus(
      PopWorkerStatus status, const std::string &runtime_env_setup_error_msg = "") {
    RAY_CHECK(status != PopWorkerStatus::OK);
    for (const auto &pair : callbacks) {
      for (const auto &callback : pair.second) {
        // No lease should be dispatched.
        ASSERT_FALSE(
            callback(nullptr,
                     status,
                     /*runtime_env_setup_error_msg*/ runtime_env_setup_error_msg));
      }
    }
    callbacks.clear();
  }

  void TriggerCallbacks() {
    for (auto it = workers.begin(); it != workers.end();) {
      std::shared_ptr<WorkerInterface> worker = *it;
      auto runtime_env_hash = worker->GetRuntimeEnvHash();
      bool dispatched = false;
      auto cb_it = callbacks.find(runtime_env_hash);
      if (cb_it != callbacks.end()) {
        auto &list = cb_it->second;
        RAY_CHECK(!list.empty());
        for (auto list_it = list.begin(); list_it != list.end();) {
          auto &callback = *list_it;
          dispatched = callback(worker, PopWorkerStatus::OK, "");
          list_it = list.erase(list_it);
          if (dispatched) {
            break;
          }
        }
        if (list.empty()) {
          callbacks.erase(cb_it);
        }
        if (dispatched) {
          it = workers.erase(it);
          continue;
        }
      }
      it++;
    }
  }

  std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const std::shared_ptr<ClientConnection> &connection) const override {
    RAY_CHECK(false) << "Not used.";
    return nullptr;
  }

  std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const std::shared_ptr<ClientConnection> &connection) const override {
    RAY_CHECK(false) << "Not used.";
    return nullptr;
  }

  void HandleJobStarted(const JobID &job_id, const rpc::JobConfig &job_config) override {
    RAY_CHECK(false) << "Not used.";
  }

  void HandleJobFinished(const JobID &job_id) override {
    RAY_CHECK(false) << "Not used.";
  }

  void Start() override { RAY_CHECK(false) << "Not used."; }

  void SetNodeManagerPort(int node_manager_port) override {
    RAY_CHECK(false) << "Not used.";
  }

  void SetRuntimeEnvAgentClient(
      std::unique_ptr<RuntimeEnvAgentClient> runtime_env_agent_client) override {
    RAY_CHECK(false) << "Not used.";
  }

  std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredDrivers(
      bool filter_dead_drivers, bool filter_system_drivers) const override {
    RAY_CHECK(false) << "Not used.";
    return {};
  }

  Status RegisterDriver(const std::shared_ptr<WorkerInterface> &worker,
                        const rpc::JobConfig &job_config,
                        std::function<void(Status, int)> send_reply_callback) override {
    RAY_CHECK(false) << "Not used.";
    return Status::Invalid("Not used.");
  }

  Status RegisterWorker(const std::shared_ptr<WorkerInterface> &worker,
                        pid_t pid,
                        std::function<void(Status, int)> send_reply_callback) override {
    RAY_CHECK(false) << "Not used.";
    return Status::Invalid("Not used.");
  }

  boost::optional<const rpc::JobConfig &> GetJobConfig(
      const JobID &job_id) const override {
    RAY_CHECK(false) << "Not used.";
    return boost::none;
  }

  void OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PushSpillWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PushRestoreWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    RAY_CHECK(false) << "Not used.";
  }

  void DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                        rpc::WorkerExitType disconnect_type) override {
    RAY_CHECK(false) << "Not used.";
  }

  void DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PrestartWorkers(const LeaseSpecification &lease_spec,
                       int64_t backlog_size) override {
    RAY_CHECK(false) << "Not used.";
  }

  void StartNewWorker(
      const std::shared_ptr<PopWorkerRequest> &pop_worker_request) override {
    RAY_CHECK(false) << "Not used.";
  }

  std::string DebugString() const override {
    RAY_CHECK(false) << "Not used.";
    return "";
  }

  void PopSpillWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PopRestoreWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PushDeleteWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PopDeleteWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    RAY_CHECK(false) << "Not used.";
  }

  size_t CallbackSize(int runtime_env_hash) {
    auto cb_it = callbacks.find(runtime_env_hash);
    if (cb_it != callbacks.end()) {
      auto &list = cb_it->second;
      return list.size();
    }
    return 0;
  }

  std::list<std::shared_ptr<WorkerInterface>> workers;
  absl::flat_hash_map<int, std::list<PopWorkerCallback>> callbacks;
  int num_pops;
};

std::shared_ptr<ClusterResourceScheduler> CreateSingleNodeScheduler(
    const std::string &id,
    double num_cpus,
    double num_gpus,
    gcs::GcsClient &gcs_client,
    ray::observability::MetricInterface &resource_usage_gauge,
    ray::ClockInterface &clock) {
  absl::flat_hash_map<std::string, double> local_node_resources;
  local_node_resources[ray::kCPU_ResourceLabel] = num_cpus;
  local_node_resources[ray::kGPU_ResourceLabel] = num_gpus;
  local_node_resources[ray::kMemory_ResourceLabel] = 128;
  absl::flat_hash_map<std::string, std::string> local_node_labels = {
      {kLabelKeyNodeID, NodeID::FromBinary(id).Hex()}};
  static instrumented_io_context io_context;
  auto scheduler = std::make_shared<ClusterResourceScheduler>(
      PeriodicalRunner::Create(io_context),
      scheduling::NodeID(id),
      local_node_resources,
      /*is_node_available_fn*/
      [&gcs_client](scheduling::NodeID node_id) {
        return !gcs_client.Nodes().IsNodeDead(NodeID::FromBinary(node_id.Binary()));
      },
      resource_usage_gauge,
      clock,
      /*get_used_object_store_memory=*/nullptr,
      /*get_pull_manager_at_capacity=*/nullptr,
      /*shutdown_raylet_gracefully=*/nullptr,
      local_node_labels);

  return scheduler;
}

RayLease CreateLease(
    const std::unordered_map<std::string, double> &required_resources,
    int num_args = 0,
    std::vector<ObjectID> args = {},
    const std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info = nullptr,
    rpc::SchedulingStrategy scheduling_strategy = rpc::SchedulingStrategy(),
    const LeaseID &lease_id = LeaseID::FromRandom(),
    const LabelSelector &label_selector = {},
    const std::vector<FallbackOption> &fallback_strategy = {}) {
  TaskSpecBuilder spec_builder;
  TaskID id = RandomTaskId();
  JobID job_id = RandomJobId();
  rpc::Address address;
  address.set_node_id(NodeID::FromRandom().Binary());
  address.set_worker_id(WorkerID::FromRandom().Binary());
  spec_builder.SetCommonTaskSpec(id,
                                 "dummy_task",
                                 Language::PYTHON,
                                 FunctionDescriptorBuilder::BuildPython("", "", "", ""),
                                 job_id,
                                 rpc::JobConfig(),
                                 TaskID::Nil(),
                                 0,
                                 TaskID::Nil(),
                                 address,
                                 0,
                                 /*returns_dynamic=*/false,
                                 /*is_streaming_generator*/ false,
                                 /*generator_backpressure_num_objects*/ -1,
                                 required_resources,
                                 {},
                                 "",
                                 0,
                                 TaskID::Nil(),
                                 "",
                                 runtime_env_info,
                                 /*concurrency_group_name=*/"",
                                 /*enable_task_events=*/true,
                                 /*labels=*/{},
                                 label_selector,
                                 fallback_strategy);

  if (!args.empty()) {
    for (auto &arg : args) {
      spec_builder.AddArg(TaskArgByReference(arg, rpc::Address(), ""));
    }
  } else {
    for (int i = 0; i < num_args; i++) {
      ObjectID put_id = ObjectID::FromIndex(RandomTaskId(), /*index=*/i + 1);
      spec_builder.AddArg(TaskArgByReference(put_id, rpc::Address(), ""));
    }
  }

  spec_builder.SetNormalTaskSpec(0, false, "", scheduling_strategy, ActorID::Nil());
  TaskSpecification spec = std::move(spec_builder).ConsumeAndBuild();
  LeaseSpecification lease_spec(spec.GetMessage());
  lease_spec.GetMutableMessage().set_lease_id(lease_id.Binary());
  return RayLease(std::move(lease_spec));
}

class FakeLeaseDependencyManager : public LeaseDependencyManagerInterface {
 public:
  explicit FakeLeaseDependencyManager(std::unordered_set<ObjectID> &missing_objects)
      : missing_objects_(missing_objects) {}

  bool RequestLeaseDependencies(const LeaseID &lease_id,
                                const std::vector<rpc::ObjectReference> &required_objects,
                                const TaskMetricsKey &task_key) {
    RAY_CHECK(subscribed_leases.insert(lease_id).second);
    for (auto &obj_ref : required_objects) {
      if (missing_objects_.find(ObjectRefToId(obj_ref)) != missing_objects_.end()) {
        return false;
      }
    }
    return true;
  }

  void RemoveLeaseDependencies(const LeaseID &lease_id) {
    RAY_CHECK(subscribed_leases.erase(lease_id));
  }

  bool LeaseDependenciesBlocked(const LeaseID &lease_id) const {
    return blocked_leases.count(lease_id);
  }

  bool CheckObjectLocal(const ObjectID &object_id) const { return true; }

  std::unordered_set<ObjectID> &missing_objects_;
  std::unordered_set<LeaseID> subscribed_leases;
  std::unordered_set<LeaseID> blocked_leases;
};

class FeatureFlagEnvironment : public ::testing::Environment {
  /// We should run these tests with feature flags on to ensure we are testing the flagged
  /// behavior.
 public:
  ~FeatureFlagEnvironment() override {}

  // Override this to define how to set up the environment.
  void SetUp() override { RayConfig::instance().worker_cap_enabled() = true; }

  // Override this to define how to tear down the environment.
  void TearDown() override {}
};

testing::Environment *const env =
    ::testing::AddGlobalTestEnvironment(new FeatureFlagEnvironment);

class ClusterLeaseManagerTest : public ::testing::Test {
 public:
  explicit ClusterLeaseManagerTest(double num_cpus_at_head = 8.0,
                                   double num_gpus_at_head = 0.0)
      : gcs_client_(std::make_unique<gcs::FakeGcsClient>()),
        id_(NodeID::FromRandom()),
        scheduler_(CreateSingleNodeScheduler(id_.Binary(),
                                             num_cpus_at_head,
                                             num_gpus_at_head,
                                             *gcs_client_,
                                             fake_resource_usage_gauge_,
                                             clock_)),
        lease_dependency_manager_(missing_objects_),
        local_lease_manager_(std::make_unique<LocalLeaseManager>(
            id_,
            *scheduler_,
            lease_dependency_manager_,
            /* get_node_info= */
            [this](
                const NodeID &node_id) -> std::optional<rpc::GcsNodeAddressAndLiveness> {
              node_info_calls_++;
              if (node_info_.count(node_id) != 0) {
                return std::optional((node_info_[node_id]));
              }
              return std::nullopt;
            },
            pool_,
            leased_workers_,
            /* get_lease_args= */
            [this](const std::vector<ObjectID> &object_ids,
                   std::vector<std::unique_ptr<RayObject>> *results) {
              for (auto &obj_id : object_ids) {
                if (missing_objects_.count(obj_id) == 0) {
                  results->emplace_back(MakeDummyArg());
                } else {
                  results->emplace_back(nullptr);
                }
              }
              return true;
            },
            /*max_pinned_lease_args_bytes=*/1000,
            /*scheduler_metrics=*/
            scheduler_metrics_,
            /*clock=*/fake_clock_)),
        lease_manager_(
            id_,
            *scheduler_,
            /* get_node_info= */
            [this](
                const NodeID &node_id) -> std::optional<rpc::GcsNodeAddressAndLiveness> {
              node_info_calls_++;
              if (node_info_.count(node_id) != 0) {
                return std::optional((node_info_[node_id]));
              }
              return std::nullopt;
            },
            /* announce_infeasible_lease= */
            [this](const RayLease &lease) { announce_infeasible_lease_calls_++; },
            *local_lease_manager_) {
    RayConfig::instance().initialize("{\"scheduler_top_k_absolute\": 1}");
  }

  void SetUp() {}

  RayObject *MakeDummyArg() {
    std::vector<uint8_t> data;
    data.resize(default_arg_size_);
    auto buffer = std::make_shared<LocalMemoryBuffer>(data.data(), data.size());
    return new RayObject(buffer, nullptr, {});
  }

  void Shutdown() {}

  void AddNode(const NodeID &id,
               double num_cpus,
               double num_gpus = 0,
               double memory = 0) {
    absl::flat_hash_map<std::string, double> node_resources;
    node_resources[ray::kCPU_ResourceLabel] = num_cpus;
    node_resources[ray::kGPU_ResourceLabel] = num_gpus;
    node_resources[ray::kMemory_ResourceLabel] = memory;
    scheduler_->GetClusterResourceManager().AddOrUpdateNode(
        scheduling::NodeID(id.Binary()), node_resources, node_resources);

    rpc::GcsNodeAddressAndLiveness info;
    node_info_[id] = info;
  }

  void AssertNoLeaks() {
    ASSERT_TRUE(lease_manager_.leases_to_schedule_.empty());
    ASSERT_TRUE(local_lease_manager_->leases_to_grant_.empty());
    ASSERT_TRUE(local_lease_manager_->waiting_leases_index_.empty());
    ASSERT_TRUE(local_lease_manager_->waiting_lease_queue_.empty());
    ASSERT_TRUE(lease_manager_.infeasible_leases_.empty());
    ASSERT_TRUE(local_lease_manager_->granted_lease_args_.empty());
    ASSERT_TRUE(local_lease_manager_->pinned_lease_arguments_.empty());
    ASSERT_TRUE(local_lease_manager_->info_by_sched_cls_.empty());
    ASSERT_EQ(local_lease_manager_->pinned_lease_arguments_bytes_, 0);
    ASSERT_TRUE(lease_dependency_manager_.subscribed_leases.empty());
  }

  void AssertPinnedLeaseArgumentsPresent(const RayLease &lease) {
    const auto &expected_deps = lease.GetLeaseSpecification().GetDependencyIds();
    ASSERT_EQ(local_lease_manager_
                  ->granted_lease_args_[lease.GetLeaseSpecification().LeaseId()],
              expected_deps);
    for (auto &arg : expected_deps) {
      ASSERT_TRUE(local_lease_manager_->pinned_lease_arguments_.count(arg));
    }
  }

  int NumLeasesToDispatchWithStatus(internal::WorkStatus status) {
    int count = 0;
    for (const auto &pair : local_lease_manager_->leases_to_grant_) {
      for (const auto &work : pair.second) {
        if (work->GetState() == status) {
          count++;
        }
      }
    }
    return count;
  }

  int NumRunningLeases() {
    int count = 0;
    for (const auto &pair : local_lease_manager_->info_by_sched_cls_) {
      count += (pair.second.granted_leases.size());
    }

    return count;
  }

  std::unique_ptr<gcs::FakeGcsClient> gcs_client_;
  NodeID id_;
  ray::observability::FakeGauge fake_resource_usage_gauge_;
  ray::Clock clock_;
  // Controllable clock used to drive the LocalLeaseManager's scheduling-class cap
  // backoff timing in tests. Declared before local_lease_manager_ so it outlives it.
  ray::FakeClock fake_clock_;
  std::shared_ptr<ClusterResourceScheduler> scheduler_;
  FakeWorkerPool pool_;
  absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> leased_workers_;
  std::unordered_set<ObjectID> missing_objects_;

  int default_arg_size_ = 10;

  int node_info_calls_ = 0;
  int announce_infeasible_lease_calls_ = 0;
  absl::flat_hash_map<NodeID, rpc::GcsNodeAddressAndLiveness> node_info_;
  ray::observability::FakeGauge fake_scheduler_tasks_gauge_;
  ray::observability::FakeGauge fake_scheduler_unscheduleable_tasks_gauge_;
  ray::observability::FakeGauge fake_scheduler_failed_worker_startup_total_gauge_;
  ray::observability::FakeGauge fake_internal_num_spilled_tasks_gauge_;
  ray::observability::FakeGauge fake_internal_num_infeasible_scheduling_classes_gauge_;
  ray::raylet::SchedulerMetrics scheduler_metrics_{
      fake_scheduler_tasks_gauge_,
      fake_scheduler_unscheduleable_tasks_gauge_,
      fake_scheduler_failed_worker_startup_total_gauge_,
      fake_internal_num_spilled_tasks_gauge_,
      fake_internal_num_infeasible_scheduling_classes_gauge_,
  };
  FakeLeaseDependencyManager lease_dependency_manager_;
  std::unique_ptr<LocalLeaseManager> local_lease_manager_;
  ClusterLeaseManager lease_manager_;
};


TEST_F(ClusterLeaseManagerTest, UnusedReplacementReservesResourcesWhileWaitingForWorker) {
  rpc::Address owner;
  owner.set_node_id(id_.Binary());
  owner.set_worker_id(WorkerID::FromRandom().Binary());
  auto task = GenActorCreationTask(JobID::FromInt(1), 1, false, "", "issue-first",
                                   owner, {{"CPU", 4}}, {{"CPU", 4}});
  LeaseSpecification spec1(task.GetMessage());
  const auto l1 = LeaseID::FromRandom(), l2 = LeaseID::FromRandom();
  spec1.GetMutableMessage().set_lease_id(l1.Binary());
  LeaseSpecification spec2(spec1.GetMessage());
  spec2.GetMutableMessage().set_lease_id(l2.Binary());
  auto w1 = std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, clock_);
  rpc::RequestWorkerLeaseReply reply1, reply2;
  int replies1=0, replies2=0;
  auto cb1=[&](Status s, auto, auto){EXPECT_TRUE(s.ok()); ++replies1;};
  auto cb2=[&](Status s, auto, auto){EXPECT_TRUE(s.ok()); ++replies2;};
  lease_manager_.QueueAndScheduleLease(RayLease(spec1), false, false,
      {internal::ReplyCallback(cb1, &reply1)});
  pool_.PushWorker(w1); pool_.TriggerCallbacks();
  ASSERT_EQ(replies1, 1);
  ASSERT_FALSE(lease_manager_.CancelLease(l1));
  ASSERT_EQ(leased_workers_.at(l1)->WorkerId(), w1->WorkerId());
  ASSERT_EQ(scheduler_->GetLocalResourceManager().GetLocalAvailableCpus(), 4.0);
  lease_manager_.QueueAndScheduleLease(RayLease(spec2), false, false,
      {internal::ReplyCallback(cb2, &reply2)});
  EXPECT_EQ(replies2, 0);
  EXPECT_EQ(leased_workers_.size(), 1u);
  EXPECT_TRUE(local_lease_manager_->IsLeaseQueued(spec2.GetSchedulingClass(), l2));
  EXPECT_EQ(scheduler_->GetLocalResourceManager().GetLocalAvailableCpus(), 0.0);
  std::cout << "RAYLET_RESOURCE_TRACE total_cpu=8 W1_lifetime_cpu=4 pending_L2_reserved_cpu=4 available_cpu="
            << scheduler_->GetLocalResourceManager().GetLocalAvailableCpus() << std::endl;
  // Counterfactual control: a real cancel releases L2's reservation, preserving W1.
  EXPECT_TRUE(lease_manager_.CancelLease(l2));
  EXPECT_EQ(replies2, 1);
  EXPECT_TRUE(reply2.canceled());
  EXPECT_EQ(scheduler_->GetLocalResourceManager().GetLocalAvailableCpus(), 4.0);
  EXPECT_EQ(leased_workers_.at(l1)->WorkerId(), w1->WorkerId());
  local_lease_manager_->ReleaseWorkerResources(w1);
  EXPECT_EQ(scheduler_->GetLocalResourceManager().GetLocalAvailableCpus(), 8.0);
  std::cout << "RAYLET_RESOURCE_TRACE cancel_L2_available=4 release_W1_available=8" << std::endl;
}
} // namespace raylet
} // namespace ray
