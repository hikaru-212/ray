// Copyright 2026 The Ray Authors.
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

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/asio/periodical_runner.h"
#include "ray/observability/fake_metric.h"
#include "ray/raylet/placement_group_resource_manager.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/util/clock.h"

namespace ray::rpc {

// Control transport separately from execution of the real raylet resource manager.
// A failed callback never mutates resources. Retaining a request separately models
// GrpcClient's InFlight fault plus delayed ServerCall::HandleRequestImpl dispatch;
// prepare and commit are still allowed to execute while removal is delayed.
class CleanupSafetyRayletClient : public FakeRayletClient {
 public:
  using Bundles = std::vector<std::shared_ptr<const BundleSpecification>>;

  explicit CleanupSafetyRayletClient(const NodeID &node_id)
      : resources_(PeriodicalRunner::Create(io_),
                   scheduling::NodeID(node_id.Binary()),
                   absl::flat_hash_map<std::string, double>{{"CPU", 2}},
                   [](auto) { return true; },
                   gauge_,
                   clock_),
        bundles_(resources_) {}

  void PrepareBundleResources(
      const Bundles &bundles,
      const ClientCallback<PrepareBundleResourcesReply> &callback) override {
    last_prepare = Snapshot(bundles);
    prepare_succeeded = bundles_.PrepareBundles(last_prepare);
    FakeRayletClient::PrepareBundleResources(bundles, callback);
  }

  void CommitBundleResources(
      const Bundles &bundles,
      const ClientCallback<CommitBundleResourcesReply> &callback) override {
    last_commit = Snapshot(bundles);
    bundles_.CommitBundles(last_commit);
    FakeRayletClient::CommitBundleResources(bundles, callback);
  }

  void RemovePlacementGroupBundles(
      const PlacementGroupID &pg_id,
      const Bundles &bundles,
      const ClientCallback<RemovePlacementGroupBundlesReply> &callback) override {
    last_remove = Snapshot(bundles);
    FakeRayletClient::RemovePlacementGroupBundles(pg_id, bundles, callback);
  }

  void RetainDelayedRemoval() { delayed_remove = last_remove; }

  Status ExecuteRemoval(const Bundles &request) {
    ++executed_removals;
    for (const auto &bundle : request) {
      auto status = bundles_.ReturnBundle(*bundle);
      if (!status.ok()) {
        return status;
      }
    }
    return Status::OK();
  }

  Status ExecuteDelayedRemoval() {
    auto request = std::move(delayed_remove);
    delayed_remove.clear();
    return ExecuteRemoval(request);
  }

  double AvailableCpu() {
    return resources_.GetLocalResourceManager().GetLocalAvailableCpus();
  }

  double BundleCpu(const BundleSpecification &bundle) {
    const auto name = "CPU_group_" + std::to_string(bundle.Index()) + "_" +
                      bundle.PlacementGroupId().Hex();
    return resources_.GetLocalResourceManager()
        .GetLocalResources()
        .available.Sum(scheduling::ResourceID(name))
        .Double();
  }

  bool prepare_succeeded = false;
  int executed_removals = 0;
  Bundles last_prepare;
  Bundles last_commit;
  Bundles last_remove;
  Bundles delayed_remove;

 private:
  static Bundles Snapshot(const Bundles &bundles) {
    Bundles result;
    for (const auto &bundle : bundles) {
      result.push_back(std::make_shared<BundleSpecification>(bundle->GetMessage()));
    }
    return result;
  }

  instrumented_io_context io_;
  Clock clock_;
  observability::FakeGauge gauge_;
  ClusterResourceScheduler resources_;
  raylet::NewPlacementGroupResourceManager bundles_;
};

}  // namespace ray::rpc
